/*
    Real Time File Streaming copyright (c) 2014 Ross Bencina

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
#include <stdio.h>
#include <math.h>
#include "portaudio.h"

#include "FileIoServer.h"
#include "FileIoStreams.h"
#include "QwMpscFifoQueue.h"

#define SAMPLE_RATE         (44100)
//#define FRAMES_PER_BUFFER   (64)
#define FRAMES_PER_BUFFER       (67) // use a prime number to flush out bugs in buffer copying code
#ifndef M_PI
#define M_PI  (3.14159265)
#endif


struct ControlCommand {
    enum LinkIndices{
        TRANSIT_NEXT_LINK_INDEX = 0,
        LINK_COUNT=1
    };
    ControlCommand *links_[LINK_COUNT];

    enum CommandType {
        START_PLAYING,
        STOP_PLAYING,
        START_RECORDING,
        STOP_RECORDING
    };

    int commandType;

    SharedBuffer *filePath; // 0 if unused

    // We'll allocate and destroy commands in the main thread.
    // We don't need construction or destruction to be real-time.

    ControlCommand( int commandType_, const char *filePathCStr=0 )
        : commandType( commandType_ )
        , filePath( 0 )
    {
        for (int i=0; i < LINK_COUNT; ++i)
            links_[i] = 0;

        // construct shared buffer in the non-real-time context
        if (filePathCStr)
            filePath = SharedBufferAllocator::alloc(filePathCStr);
    }

    ~ControlCommand()
    {
        if (filePath) {
            filePath->release();
            filePath = 0;
        }
    }
};

typedef QwMpscFifoQueue<ControlCommand*,ControlCommand::TRANSIT_NEXT_LINK_INDEX> control_command_queue_t;


#define TABLE_SIZE   (200)
typedef struct
{
    // sine oscillator so we can hear whether the stream is glitching
    float sine[TABLE_SIZE];
    int left_phase;
    int right_phase;

    //

    control_command_queue_t commandsToAudioCallback;
    control_command_queue_t commandsFromAudioCallback;

    READSTREAM *playStream;
    WRITESTREAM *recordStream;
}
TestStreamCallbackData;

/* This routine will be called by the PortAudio engine when audio is needed.
** It may called at interrupt level on some machines so don't do anything
** that could mess up the system like calling malloc() or free().
*/
static int audioCallback( const void *inputBuffer, void *outputBuffer,
                            unsigned long framesPerBuffer,
                            const PaStreamCallbackTimeInfo* timeInfo,
                            PaStreamCallbackFlags statusFlags,
                            void *userData )
{
    TestStreamCallbackData *data = (TestStreamCallbackData*)userData;
    float *out = (float*)outputBuffer; // interleaved left and right channels
    unsigned long i;

    (void) timeInfo; /* Prevent unused variable warnings. */
    (void) statusFlags;
    (void) inputBuffer;
    
    // output a sine tone when we're not playing
    float sineGain = 0.1f; // quiet
    for( i=0; i<framesPerBuffer; i++ )
    {
        *out++ = data->sine[data->left_phase] * sineGain;  /* left */
        *out++ = data->sine[data->right_phase] * sineGain;  /* right */
        data->left_phase += 1;
        if( data->left_phase >= TABLE_SIZE ) data->left_phase -= TABLE_SIZE;
        data->right_phase += 3; /* higher pitch so we can distinguish left and right. */
        if( data->right_phase >= TABLE_SIZE ) data->right_phase -= TABLE_SIZE;
    }


    // receive commands from the main thread. demostrates creating and destroying streams in the real-time thread:

    while (ControlCommand *cmd = data->commandsToAudioCallback.pop()) {
        switch (cmd->commandType) {
        case ControlCommand::START_PLAYING:
            assert( !data->playStream );
            data->playStream = FileIoReadStream_open(cmd->filePath, FileIoRequest::READ_ONLY_OPEN_MODE);
            break;
        case ControlCommand::STOP_PLAYING:
            if (data->playStream) { // idempotent to simplify logic in main()
                FileIoReadStream_close(data->playStream);
                data->playStream = 0;
            }
            break;
        case ControlCommand::START_RECORDING:
            assert( !data->recordStream );
            data->recordStream = FileIoWriteStream_open(cmd->filePath, FileIoRequest::READ_WRITE_OVERWRITE_OPEN_MODE);
            break;
        case ControlCommand::STOP_RECORDING:
            if (data->recordStream) { // idempotent to simplify logic in main()
                FileIoWriteStream_close(data->recordStream);
                data->recordStream = 0;
            }
            break;
        }

        data->commandsFromAudioCallback.push(cmd); // return command to main thread for deletion
    }

    // looped playback of the playStream, if it exists
    if (data->playStream) {
        FileIoStreamState streamState = FileIoReadStream_pollState(data->playStream);
        if (streamState==STREAM_STATE_OPEN_IDLE || streamState==STREAM_STATE_OPEN_EOF) {

            // Once the file is open, seek to play from start. also, when it reaches EOF, loop
            FileIoReadStream_seek(data->playStream,0);

        } else if (streamState==STREAM_STATE_OPEN_STREAMING) { // (don't output in BUFFERING state)

            short temp[FRAMES_PER_BUFFER * 2]; // stereo 16 bit file data
            size_t framesRead = FileIoReadStream_read( temp, sizeof(short)*2, FRAMES_PER_BUFFER, data->playStream );

            // Copy data to output
            float gain = 1./32768;

            out = (float*)outputBuffer;
            const short *p = &temp[0];
            for( size_t i=0; i < framesRead; ++i ) {
                *out++ = *p++ * gain;
                *out++ = *p++ * gain;
            }
        }
    }
    
    if (data->recordStream) {
        FileIoStreamState streamState = FileIoWriteStream_pollState(data->recordStream);
        if (streamState==STREAM_STATE_OPEN_IDLE) {

            // Once the file is open, seek to record from start. This allocates buffers.
            FileIoWriteStream_seek(data->recordStream,0);

        } else if (streamState==STREAM_STATE_OPEN_STREAMING) { // (don't record in BUFFERING state)
            short temp[FRAMES_PER_BUFFER * 2]; // stereo 16 bit file data

            // Copy data from input
            float gain = 32767;

            float *in = (float*)inputBuffer;
            short *p = &temp[0];
            for( size_t i=0; i < FRAMES_PER_BUFFER; ++i ) {
                *p++ = (short)(*in++ * gain);
                *p++ = (short)(*in++ * gain);
            }

            FileIoWriteStream_write( temp, sizeof(short)*2, FRAMES_PER_BUFFER, data->recordStream );
        }
    }
    
    return paContinue;
}


/*******************************************************************/
int main(int argc, char *argv[]);
int main(int argc, char *argv[])
{
    PaError err;
    TestStreamCallbackData paStreamData;
    
    /* initialise sinusoidal wavetable */
    for( int i=0; i<TABLE_SIZE; i++ )
    {
        paStreamData.sine[i] = (float) sin( ((double)i/(double)TABLE_SIZE) * M_PI * 2. );
    }
    paStreamData.left_phase = paStreamData.right_phase = 0;
    
    // Play data from a 16-bit stereo headerless file specified by the following path. sample rate should be SAMPLE_RATE.
    //const char *playbackFilePath = "C:\\Users\\Ross\\Desktop\\07_Hungry_Ghost.dat";
    const char *playbackFilePath = "C:\\RealTimeSafeStreamingFileIoExample\\RealTimeSafeStreamingFileIoExample\\164718__bradovic__piano.dat";

    // Record to a temp file:
    const char *recordTestFilePath = "C:\\Users\\Ross\\Desktop\\FileStreamingTestRecording.dat";

    paStreamData.playStream = 0;
    paStreamData.recordStream = 0;

    startFileIoServer();
    FileIoReadStream_test();
    FileIoWriteStream_test();

    err = Pa_Initialize();
    if( err != paNoError ) goto error;

    PaStreamParameters inputParameters;
    inputParameters.device = Pa_GetDefaultInputDevice();
    if (inputParameters.device == paNoDevice) {
        fprintf(stderr,"Error: No default input device.\n");
        err = paInvalidDevice;
        goto error;
    }
    inputParameters.channelCount = 2;       /* stereo output */
    inputParameters.sampleFormat = paFloat32; /* 32 bit floating point output */
    inputParameters.suggestedLatency = Pa_GetDeviceInfo( inputParameters.device )->defaultLowOutputLatency;
    inputParameters.hostApiSpecificStreamInfo = NULL;

    PaStreamParameters outputParameters;
    outputParameters.device = Pa_GetDefaultOutputDevice();
    if (outputParameters.device == paNoDevice) {
      fprintf(stderr,"Error: No default output device.\n");
      err = paInvalidDevice;
      goto error;
    }
    outputParameters.channelCount = 2;       /* stereo output */
    outputParameters.sampleFormat = paFloat32; /* 32 bit floating point output */
    outputParameters.suggestedLatency = Pa_GetDeviceInfo( outputParameters.device )->defaultLowOutputLatency;
    outputParameters.hostApiSpecificStreamInfo = NULL;

    PaStream *stream;
    err = Pa_OpenStream(
              &stream,
              &inputParameters,
              &outputParameters,
              SAMPLE_RATE,
              FRAMES_PER_BUFFER,
              paNoFlag,
              audioCallback,
              &paStreamData );
    if( err != paNoError ) goto error;

    err = Pa_StartStream( stream );
    if( err != paNoError ) goto error;

    // process user input...

    printf("Press p+[Enter] to toggle playing the playback file (%s).\n", playbackFilePath);
    printf("Press r+[Enter] to toggle recording the test file (%s).\n", recordTestFilePath);
    printf("Press t+[Enter] to toggle playing the test file (%s).\n", recordTestFilePath);
    printf("Press q+[Enter] to quit.\n");

    enum State { IDLE, PLAYING_PLAYBACK_FILE, RECORDING_TEST_FILE, PLAYING_TEST_FILE };
    State state = IDLE;

    bool quit=false;
    while(!quit) {
        int c = getc(stdin);
        if (c =='q') {
            quit=true;

        } else if (c=='p') { // play or stop playing the playback file
            if (state==PLAYING_PLAYBACK_FILE) {
                printf("STOP playing playback file (stopping)\n");
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_PLAYING));
                state = IDLE;
            } else {
                printf("PLAY playback file (starting)\n");
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_PLAYING));
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_RECORDING));
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::START_PLAYING, playbackFilePath));
                state = PLAYING_PLAYBACK_FILE;
            }

        } else if (c=='r') {
            if (state==RECORDING_TEST_FILE) {
                printf("STOP recording test-recording file (stopping)\n");
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_RECORDING));
                state = IDLE;
            } else {
                printf("RECORD test-recording file (starting)\n");
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_PLAYING));
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_PLAYING));
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::START_RECORDING, recordTestFilePath));
                state = RECORDING_TEST_FILE;
            }

        } else if (c=='t') {
            if (state==PLAYING_TEST_FILE) {
                printf("STOP playing test-recording file (stopping)\n");
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_PLAYING));
                state = IDLE;
            } else {
                printf("PLAY test-recording file (starting)\n");
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_PLAYING));
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::STOP_RECORDING));
                paStreamData.commandsToAudioCallback.push(new ControlCommand(ControlCommand::START_PLAYING, recordTestFilePath));
                state = PLAYING_TEST_FILE;
            }
        }

        // reclaim used commands
        while (ControlCommand *cmd = paStreamData.commandsFromAudioCallback.pop())
            delete cmd;
    }

    err = Pa_StopStream( stream );
    if( err != paNoError ) goto error;

    err = Pa_CloseStream( stream );
    if( err != paNoError ) goto error;

    Pa_Terminate();
    
    // reclaim used commands
    while (ControlCommand *cmd = paStreamData.commandsFromAudioCallback.pop())
        delete cmd;

    if (paStreamData.playStream) {
        FileIoReadStream_close(paStreamData.playStream);
        paStreamData.playStream = 0;
    }

    if (paStreamData.recordStream) {
        FileIoWriteStream_close(paStreamData.recordStream);
        paStreamData.recordStream = 0;
    }

    shutDownFileIoServer();
    SharedBufferAllocator::reclaimMemory();
    SharedBufferAllocator::checkForLeaks();

    printf("Test finished.\n");

    return err;
error:
    Pa_Terminate();

    shutDownFileIoServer();
    SharedBufferAllocator::reclaimMemory();

    fprintf( stderr, "An error occured while using the portaudio stream\n" );
    fprintf( stderr, "Error number: %d\n", err );
    fprintf( stderr, "Error message: %s\n", Pa_GetErrorText( err ) );
    return err;
}



/*
TODO:

----------------------

another demo would be a multi-file-player demo:

basically it can play multiple files. you queue them up from the main thread by typing keys a, b, c, d etc.
It keeps the PA stream open while there is at least one file playing, but it can play more than one at once.

----------------------

*/
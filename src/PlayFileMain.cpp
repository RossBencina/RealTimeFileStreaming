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
#include "FileIoReadStream.h" // rename FileIoReadStream?

#define SAMPLE_RATE         (44100)
#define FRAMES_PER_BUFFER   (64)

#ifndef M_PI
#define M_PI  (3.14159265)
#endif

#define TABLE_SIZE   (200)
typedef struct
{
    // sine oscillator so we can hear whether the stream is glitching
    float sine[TABLE_SIZE];
    int left_phase;
    int right_phase;

    //

    volatile bool doPlayFile; // signal audio callback to start playing. HACK: use of atomic flag for signaling + abuse of volatile. don't do this at home.

    SharedBuffer *readFilePath;
    READSTREAM *fileReadStream;
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
    
    // sine signal
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


    // file read stream

    if (data->doPlayFile)
    {
        if (!data->fileReadStream)
            data->fileReadStream = FileIoReadStream_open(data->readFilePath, FileIoRequest::READ_ONLY_OPEN_MODE);
    
        short temp[FRAMES_PER_BUFFER * 2]; // stereo 16 bit file data

        FileIoReadStreamState streamState = FileIoReadStream_pollState(data->fileReadStream);
        if (streamState==READSTREAM_STATE_OPEN_IDLE) {

            // once the file is open, seek to play from start
            FileIoReadStream_seek(data->fileReadStream,0);

        } else if (streamState==READSTREAM_STATE_OPEN_STREAMING) { // (don't output in BUFFERING state)

            size_t framesRead = FileIoReadStream_read( temp, sizeof(short)*2, FRAMES_PER_BUFFER, data->fileReadStream );
        
            // copy data to output
            float gain = 1./32768;

            out = (float*)outputBuffer;
            const short *p = &temp[0];
            for( size_t i=0; i < framesRead; ++i ) {
                *out++ = *p++ * gain;
                *out++ = *p++ * gain;
            }
        
        } else if (streamState==READSTREAM_STATE_ERROR || streamState==READSTREAM_STATE_OPEN_EOF) {
            FileIoReadStream_close(data->fileReadStream);
            data->fileReadStream = 0;
        }
    }
    else // !data->doPlayFile
    {
        if (data->fileReadStream) {
            FileIoReadStream_close(data->fileReadStream);
            data->fileReadStream = 0;
        }        
    }
        
    return paContinue;
}


/*******************************************************************/
int main(int argc, char *argv[]);
int main(int argc, char *argv[])
{
    PaStreamParameters outputParameters;
    PaStream *stream;
    PaError err;
    TestStreamCallbackData paStreamData;
    int i;
    
    /* initialise sinusoidal wavetable */
    for( i=0; i<TABLE_SIZE; i++ )
    {
        paStreamData.sine[i] = (float) sin( ((double)i/(double)TABLE_SIZE) * M_PI * 2. );
    }
    paStreamData.left_phase = paStreamData.right_phase = 0;
    
    paStreamData.doPlayFile = false;
    // Play data from a 16-bit stereo headerless file specified by the following path. sample rate should be SAMPLE_RATE.
    paStreamData.readFilePath = SharedBufferAllocator::alloc("C:\\Users\\Ross\\Desktop\\07_Hungry_Ghost.dat");
    paStreamData.fileReadStream = 0;


    startFileIoServer();
    FileIoReadStream_test();

    err = Pa_Initialize();
    if( err != paNoError ) goto error;

    outputParameters.device = Pa_GetDefaultOutputDevice(); /* default output device */
    if (outputParameters.device == paNoDevice) {
      fprintf(stderr,"Error: No default output device.\n");
      err = paInvalidDevice;
      goto error;
    }
    outputParameters.channelCount = 2;       /* stereo output */
    outputParameters.sampleFormat = paFloat32; /* 32 bit floating point output */
    outputParameters.suggestedLatency = Pa_GetDeviceInfo( outputParameters.device )->defaultLowOutputLatency;
    outputParameters.hostApiSpecificStreamInfo = NULL;

    err = Pa_OpenStream(
              &stream,
              NULL, /* no input */
              &outputParameters,
              SAMPLE_RATE,
              FRAMES_PER_BUFFER,
              paNoFlag,
              audioCallback,
              &paStreamData );
    if( err != paNoError ) goto error;

    err = Pa_StartStream( stream );
    if( err != paNoError ) goto error;

    printf("Press p+[Enter] to toggle playing the file.\n");
    printf("Press q+[Enter] to quit.\n");

    bool quit=false;
    while(!quit) {
        int c = getc(stdin);
        if (c =='q') {
            quit=true;
        } else if (c=='p') {
            paStreamData.doPlayFile = !paStreamData.doPlayFile;
            printf("play file: %s\n", (paStreamData.doPlayFile)?"ON":"OFF");       
        }
    }


    err = Pa_StopStream( stream );
    if( err != paNoError ) goto error;

    err = Pa_CloseStream( stream );
    if( err != paNoError ) goto error;

    Pa_Terminate();
    
    if (paStreamData.fileReadStream) {
        FileIoReadStream_close(paStreamData.fileReadStream);
        paStreamData.fileReadStream = 0;
    }

    paStreamData.readFilePath->release();
    shutDownFileIoServer();
    SharedBufferAllocator::reclaimMemory();
    SharedBufferAllocator::checkForLeaks();

    printf("Test finished.\n");

    return err;
error:
    Pa_Terminate();

    paStreamData.readFilePath->release();
    shutDownFileIoServer();
    SharedBufferAllocator::reclaimMemory();

    fprintf( stderr, "An error occured while using the portaudio stream\n" );
    fprintf( stderr, "Error number: %d\n", err );
    fprintf( stderr, "Error message: %s\n", Pa_GetErrorText( err ) );
    return err;
}



/*
TODO:

The next step in testing is:

    x- create a portaudio stream that plays a sine tone out the default outs using 16 bit stereo

    x- have a global flag called "playing_". the callback polls playing_, and when it is set, it creates a stream, seeks it to zero, and plays it. (the path gets allocated in main()). 
if playing is not set the stream is closed

    x- the callback mixes in a sine wave so you can hear if it's glitching


--

    o- r performs a similar task but it records the input.

    o- recording_

    o- q works the same as p, but plays back the recorded file.

    o- playingRecording_


I think it's probably worth having separate examples for separate cases. the above is good for fully real time in-the-stream processing


----------------------

another demo would be a multi-file-player demo:

basically it can play multiple files. you queue them up from the main thread by typing keys a, b, c, d etc.
It keeps the PA stream open while there is at least one file playing, but it can play more than one at once.

----------------------

*/
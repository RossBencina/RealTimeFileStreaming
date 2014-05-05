RealTimeFileStreaming
=====================

Example of interfacing PortAudio real time audio with file I/O

***UNDER CONSTRUCTION***

This is example code that I'm working on for a conference paper and some blog posts. It probably won't make much sense without the documentation, which will be available by July. Until then feel free to email me with questions. -- Ross.

Status
------

The code works on OS X and Windows. It has been tested on Windows 7 with MSVC10 and OS X 10.7.5 with Xcode 4.6.1.

There's a little bit of work to do to get it working on Linux (replace Mach semaphore_t with sem_t). Maybe a bit more work to get it running on ARM (the interlocked exchange atomic primitive might need some work, not sure).


Source code overview
--------------------

`FileIoStreams.h/.cpp` client stream objects for streaming data to or from a file. Lock-free and real-time safe.

`FileIoRequest.h` asynchronous message node object. Represents requests to, and replies from, the I/O server thread.

`FileIoServer.h/.cpp` file I/O server thread. Responds to FileIoRequests from client streams.

`DataBlock.h` buffer descriptor. Represents blocks of data read/written from/to a file. Pointers to DataBlocks are passed between server and client in FileIoRequest messages.

`SharedBuffer.h/.cpp` reference counted immutable shared buffer with lock-free cleanup. Used for storing file paths. 

`RecordAndPlayFileMain.cpp` example real-time audio program that records and plays raw 16-bit stereo files.



How to build and run the example
--------------------------------

*At the moment there are project files for Windows MSVC10 and OS X (Xcode 4.6 or later). Help with Linux would be welcome.*

1. Check out the sources and the dependencies:

 ```
 git clone https://github.com/RossBencina/RealTimeFileStreaming.git
 git clone https://github.com/RossBencina/QueueWorld.git
 git clone https://github.com/mintomic/mintomic.git
 svn co https://subversion.assembla.com/svn/portaudio/portaudio/trunk/ portaudio
 ```

 You should now have the following directories:

 ```
  RealTimeFileStreaming/
  QueueWorld/
  mintomic/
  portaudio/
 ```


2. Open the project.
 - On Windows, with MSVC2010 or later, navigate to `RealTimeFileStreaming\build\msvs10\RealTimeFileStreaming` and open the Visual Studio solution file `RealTimeFileStreaming.sln`
 - On OS X, with Xcode 4.6 or later, navigate to `RealTimeFileStreaming\build\xcode4.6\RealTimeFileStreaming` and open the Xcode project `RealTimeFileStreaming.xcodeproj`


3. Set up audio file paths. The example program references two file paths: `playbackFilePath` for playing an existing file and `recordTestFilePath` for playing and recording a test file. These are both declared in `main()` in `RecordAndPlayFileMain.cpp`. You need to edit their values to refer to valid file paths on your system (don't forget to escape backslashes if you're on Windows). The example code only reads headerless 16-bit stereo files (44.1k for the default settings).
 - For `playbackFilePath` you either need to create a file of the appropriate format, or you can grab this file: https://www.dropbox.com/s/ec923lzkr9udxww/171326__bradovic__piano-improvisation.dat [1].
 - The `recordTestFilePath` file should be a valid path on your system, but the file should *not* exist. It will be created or overwritten every time you start recording from within the test program. Just make sure that the directory portion of the path exists.


4. Run the project. It should build and run, playing a sine wave. There are instructions on the screen for starting and stopping recording and playback. It will work for recording and playing a new test file (`recordTestFilePath`) even if you don't have the `playbackFilePath` file set up correctly.


[1] File credit: Bradovic at freesound.org https://www.freesound.org/people/Bradovic/sounds/171326/ (CC-Zero).

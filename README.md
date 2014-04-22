RealTimeFileStreaming
=====================

Example of interfacing PortAudio real time audio with file I/O

***UNDER CONSTRUCTION***

This is example code that I'm working on for a conference paper and some blog posts. It probably won't make much sense without the documentation, which will be available by July. Until then feel free to email me with questions. -- Ross.

Source code overview
--------------------

`FileIoReadStream.h/.cpp` a client stream object, used for streaming data read from disk. Lock-free and real-time safe. 

`FileIoRequest.h` asynchronous message node object. Used to represent requests to, and replies from, the I/O server thread.

`FileIoServer.h/.cpp` file I/O server thread. Responds to FileIoRequests from client streams.

`DataBlock.h` buffer descriptor. Used to represent blocks of data read from disk. Pointers to DataBlocks are passed between server and client in FileIoRequest messages.

`SharedBuffer.h/.cpp` reference counted immutable shared buffer with lock-free cleanup. Used for storing file paths. 

`PlayFileMain.cpp` example real-time audio program.



How to build and run the example
--------------------------------

*Right now there is only a project file for Windows MSVC10, sorry. OS X coming soon. Help with Linux welcome.*

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

On Windows, with MSVC2010 or later, navigate to `RealTimeFileStreaming\build\msvs10\RealTimeFileStreaming` and open the Visual Studio solution file RealTimeFileStreaming.sln

Run the project. It should build and run, playing a sine wave. There are instructions on the screen for starting and stopping recording and playback.

To play an existing file you need to provide one. The source code only reads headerless 16-bit stereo files (44.1k for the default settings). You should create such a file and then code its name into the main() function in PlayFileMain.cpp, in the line that reads `paStreamData.readFilePath = SharedBufferAllocator::alloc("C:\\Users\\Ross...");`

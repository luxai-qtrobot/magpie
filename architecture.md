## Requirements:
1. Interrupting ongoing action(s)
    - pausing/resuming current action (curent frame  or current frame group)
    - canceling current action (curent frame  or current frame group)
    - suspending/resuming the node (will no lonnger process any incoming frames until it is resumed)
2. Get informed when an action is done: 
    - receiving relevant event from module event stream when curent frame or current frame group processing is done \
3. Handle multiple inouts/outputs for streamed data, events and commands 


## Challenges
- Reduce the number of sockets/tcp ports. Ideally having single endpoint for all communication. 


## Compont model
1. Data stream input (zeromq SUB)
2. Data stream output (zeromq PUB)
3. Event stream output (zeromq PUB)
4. Commands interface (zeromq ROUTER)


## some Solutions
- Frame grouping: set of relevant frames cane be group in single frame-group using specific ULID. 
  frames within the group can have increasing id: 0, 1, 2... 
  Frame grouping allows us to get informed and take propoer action on the relevant data frames. 
  For example, a TTS frame group can consist of multiple sentence frames. we can pause/cancel the whole tts frame group or specific sentence frame. 
- A publisher/producers of streamed frame, must mark the last frame in the group to let the consumer knows that frame group streaming is finished.  
- A frame consits of FID (frame id), GID (group id), Propoerty(meta data) and Data. Type is not required and python isinstance() method can be used 
  to get the type of the 

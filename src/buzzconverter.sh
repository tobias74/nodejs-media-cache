#!/bin/bash
 
if [[ $1 && $2 ]]
then
    filename=$(basename "$1")
    filename=${filename%.*}
    directory=$(dirname "$1")
    targetBasename=$(basename "$2")
    targetDirectory=$(dirname "$2")
    

    echo "Converting $filename to ogv"
    avconv -i "$1" -acodec libvorbis -ac 2 -ab 192k -ar 44100 -b:v 2000k "$targetDirectory/$targetBasename.ogv"
    echo "Finished ogv"
 
    echo "Converting $filename to webm"
    avconv -i "$1" -acodec libvorbis -ac 2 -ab 192k -ar 44100 -b:v 2000k "$targetDirectory/$targetBasename.webm"
    echo "Finished webm"
 
    echo "Converting $filename to h264"
    avconv -i "$1" -acodec libvo_aacenc -ac 2 -ab 192k -vcodec libx264 -level 21 -refs 2 -b:v 2000k -threads 0 "$targetDirectory/$targetBasename.mp4"
    echo "Finished h264"


    echo "Generating thumbnail"
    avconv -i "$1"  -filter:v yadif -ss 0.5 -t 1 -f image2 -frames:v 1 "$targetDirectory/$targetBasename.jpg"
    echo "Finished thumbnail"


 
    echo "All Done!"
else
    echo "Usage: [filename] [targetFilename]"
fi



#!/bin/sh
# Regenerates the media test corpus. Checked in so the bytes never drift; run
# only to add a format. Sizes are chosen against Dim::normalized's buckets
# (32x32, 96x96, 320x240, 640x480, 800x600): 1x1 upscales at every bucket and
# so reaches the passthrough branch, 100x100 straddles them, and 1000x800 is
# downscaled by all of them. 1200x900 covers the largest request the suite
# makes, which no bucket does.
set -eu
FF="ffmpeg -hide_banner -loglevel error -y"

flat() { $FF -f lavfi -i "color=c=$3:s=$2" -frames:v 1 "$1"; }
anim() { $FF -f lavfi -i "testsrc=size=$2:rate=3:duration=1" "$1"; }

flat still_1x1.png       1x1       red
flat still_100x100.png   100x100   red
flat still_1000x800.png  1000x800  red
flat still_100x100.jpg   100x100   green
$FF -f lavfi -i "color=c=blue:s=100x100"  -frames:v 1 -c:v libwebp still_100x100.webp
$FF -f lavfi -i "color=c=blue:s=1000x800" -frames:v 1 -c:v libwebp still_1000x800.webp
$FF -f lavfi -i "color=c=teal:s=1x1"      -frames:v 1 still_1x1.gif

anim anim_100x100.gif  100x100
# a flat two-tone animation, since testsrc at this size costs 100x the bytes
$FF -f lavfi -i "color=c=red:s=1000x800:d=1:r=3" -vf "geq=r='if(lt(mod(N,2),1),255,0)':g=0:b=0" anim_1000x800.gif
# larger than the largest request, so a still standing in for it has room to
# carry the size that was asked for
$FF -f lavfi -i "color=c=red:s=1200x900:d=1:r=3" -vf "geq=r='if(lt(mod(N,2),1),255,0)':g=0:b=0" anim_1200x900.gif
$FF -f lavfi -i "testsrc=size=100x100:rate=3:duration=1" -c:v libwebp_anim -loop 0 anim_100x100.webp
$FF -f lavfi -i "testsrc=size=100x100:rate=3:duration=1" -c:v apng -plays 0 anim_100x100.apng

printf 'this is not a picture\n' > notimage.txt
head -c 60 still_100x100.png > truncated.png
# enough for the header to parse but not for every frame, so the frame counter
# has a genuine failure to report
head -c 1400 anim_100x100.gif > truncated.gif

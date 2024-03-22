import glob
import os

from PIL import Image

for filename in glob.glob("images/*.jpeg"):
    im = Image.open(filename)
    outfile = os.path.splitext(filename)[0] + ".jpg"
    im.thumbnail((250, 250), Image.Resampling.LANCZOS)
    exif = im.info["exif"]
    im.save(outfile, optimize=True, quality=65, exif=exif)

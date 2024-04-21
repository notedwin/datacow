# copy over scheduled folder to the ssh: notedwin@spark 
rsync -avz scheduled/ notedwin@spark:/home/notedwin/scheduled
# copy over poetry files and dockerfile to the ssh: notedwin@spark
rsync -avz poetry.lock pyproject.toml Dockerfile notedwin@spark:/home/notedwin/scheduled

# build the docker image
ssh notedwin@spark "cd /home/notedwin/scheduled && sudo docker build -t scheduled ."

# run the docker image
ssh notedwin@spark "sudo docker run -d --name scheduled scheduled"
import requests

url = "https://api.artic.edu/api/v1/artworks/129884"
filename = "image.jpeg"
response = requests.get(url, stream=True)
response.raw.decode_content = True
with open(f"{filename}", "wb") as outfile:
    outfile.write(response.content)

# random artwork -> get image id -> call iif api -> download

import requests
from fake_useragent import UserAgent
import pandas as pd
from bs4 import BeautifulSoup

# Set headers and user agent
headers = {
    'User-Agent': UserAgent().firefox
}

# Send request to the webpage
url = 'https://fbref.com/en/players/1f44ac21/matchlogs/2022-2023/Erling-Haaland-Match-Logs'
response = requests.get(url, headers=headers)

# Parse the HTML content
soup = BeautifulSoup(response.content, 'html.parser')

# Find the table element
table = soup.find('table', {'id': 'matchlogs_all'})

# Get the table headers
headers = []
# header_row = table.find('thead').find_all('th')
# only get the text from headers not in tr with class="over_header"
header_row = table.find('thead').find('tr', {'class': None}).find_all('th')


for header in header_row:
    headers.append(header.get('aria-label'))

print(headers)

# Get the table rows
rows = []
data_rows = table.find('tbody').find_all('tr')
for row in data_rows:
    # data = row.find_all('td')
    # find all td and th
    data = row.find_all(['td', 'th'])

    row_data = [cell.text.strip() for cell in data]
    # filter out last column
    rows.append(row_data)

# Create a pandas dataframe
df = pd.DataFrame(rows, columns=headers)

# Print the dataframe
print(df)

# Save the dataframe to a csv file
df.to_csv('haaland.csv', index=False)
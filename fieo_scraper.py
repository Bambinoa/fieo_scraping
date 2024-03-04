import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import warnings
import time
import concurrent.futures
from tqdm import tqdm

warnings.filterwarnings('ignore')

def url_generator(priority_list):
    url_list = []
    for priority in priority_list: 
        url = f"https://fieo.org/searchItcHcCode_fieo.php?searchStringProducts={priority}&stype=Like&Submit=Search"
        url_list.append(url)
    return url_list

def priority_scrapper(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find the table with the desired headers
    desired_headers = ['Members', 'Products', 'ITC-HS Code', 'Certificate', 'Profile', 'Web Site']
    desired_table = None
    for table in soup.find_all('table', class_='textb'):
        header_cells = table.find_all('td', align='center')
        headers = [cell.get_text(strip=True) for cell in header_cells if cell.find('b')]
        if headers == desired_headers:
            desired_table = table
            # break
    
    # Extract table data
    if desired_table:
        table_data = []
        for row in desired_table.find_all('tr')[1:]:  # Skip the header row
            row_data = []
            for cell in row.find_all('td'):
                if cell.find('a'):  # Check if the cell contains an anchor tag
                    href = cell.find('a')['href']  # Extract the href attribute
                    href_with_prefix = 'fieo.org/' + href  # Add the prefix
                    row_data.append(href_with_prefix)    
                
                else:
                    row_data.append(cell.get_text(strip=True))
            
            table_data.append(row_data)
    return desired_headers, table_data

def certificate_data(url):
    response1 = requests.get(f'https://{url}')
    # print(f'https://{url}')
    soup = BeautifulSoup(response1.content, 'html.parser')
    target_section = soup.find('strong', {'data-info': 'memberName'}).parent.parent
    # Extract address, city, and state
    address = target_section.find('span', {'data-info': 'address'}).text.strip()
    city = target_section.find('span', {'data-info': 'city'}).text.strip()
    state = target_section.find('span', {'data-info': 'state'}).text.strip()

    # Store them in a list
    return [address, city, state]

def create_star_rating(df, column_name):
    # Define a dictionary to map endings to star ratings
    rating_mapping = {
        'STH': '4',
        'TH': '3',
        'SEH': '2',
        'PTH': '5'
    }
    
    # Extract endings from column values
    endings = df[column_name].str.extract(r'\[(.*?)\]', expand=False)
    
    # Create a new column 'Star Rating' based on endings
    df['Star Rating'] = endings.map(rating_mapping)
    
    return df

#Main Code
start_time = time.time()
list_ = [9, 17, 18, 21, 22, 28, 29, 34, 38, 39, 40, 42, 44, 48,52,57, 64, 65, 66, 70, 73, 85,94,95,96]  # Your priority list
urls = url_generator(list_)
full_data = []
address_data = []
for url in urls:
    desired_headers, table_data = priority_scrapper(url)
    full_data.extend(table_data)
    print(url)

# Convert to DataFrame
df = pd.DataFrame(full_data, columns=desired_headers) 
# df = df.head(50)
end_time = time.time()
print('Shape of Complete dataframe: ', df.shape)
print (f'Time Taken: {end_time - start_time} seconds')


# Certificate Requests
certificate_start_time = time.time()
certificate_list = df['Certificate'].to_list()
address_list = []

print("Certificate Scraping started (Mutltithreaded Process) ...")

total_certificates = len(certificate_list)
print(f"Total certificates to process: {total_certificates}")

# Function to scrape certificate data
def scrape_certificate_data(certificate):
    try:
        return certificate_data(certificate)
    except Exception as e:
        # print(f"An error occurred while processing certificate: {e}")
        return None

# Use concurrent.futures for multi-threading
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(tqdm(executor.map(scrape_certificate_data, certificate_list), desc="Progress", total=total_certificates))

# Append non-None results to address_list
for result in results:
    if result is not None:
        address_list.append(result)

certificate_end_time = time.time()

print(f"Time Taken for Certificate Scraping: {(certificate_end_time - certificate_start_time) / 3600} hours")

new_df = pd.DataFrame(address_list, columns = ['Address', 'City', 'State'])
result = pd.concat([df, new_df], axis=1)
result = create_star_rating(result, 'Members')
result.drop(columns=['Certificate', 'Profile', 'Web Site'], inplace=True)
result['State'] = result['State'].replace('', pd.NA).fillna(result['City'])
result.to_csv('Fieo_data.csv')

c_finish_time = time.time()
print ("Csv is exported")
print(f"Total time taken: {c_finish_time - start_time / 3600} hours")
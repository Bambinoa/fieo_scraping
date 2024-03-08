import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import warnings
import time
import concurrent.futures
from tqdm import tqdm
import configparser
import pushbullet

warnings.filterwarnings('ignore')

def url_generator(priority_list):
    url_list = []
    for priority in priority_list: 
        url = f"https://fieo.org/searchItcHcCode_fieo.php?searchStringProducts={priority}&stype=Like&Submit=Search"
        url_list.append(url)
    return url_list

def priority_scrapper(url):
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            desired_headers = ['Members', 'Products', 'ITC-HS Code', 'Certificate', 'Profile', 'Web Site']
            desired_table = None
            for table in soup.find_all('table', class_='textb'):
                header_cells = table.find_all('td', align='center')
                headers = [cell.get_text(strip=True) for cell in header_cells if cell.find('b')]
                if headers == desired_headers:
                    desired_table = table
            if desired_table:
                table_data = []
                for row in desired_table.find_all('tr')[1:]:
                    row_data = []
                    for cell in row.find_all('td'):
                        if cell.find('a'):
                            href = 'fieo.org/' + cell.find('a')['href']
                            row_data.append(href)
                        else:
                            row_data.append(cell.get_text(strip=True))
                    table_data.append(row_data)
                return desired_headers, table_data
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            print("Retrying...")
            time.sleep(5)

def certificate_data(url):
    time.sleep(4)
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

def send_notification(title, body):
    # config = configparser.ConfigParser()
    # config.read(r"C:\creds\pushbullet.ini")
    # api_key = config['api token']['access_token']
    pb = pushbullet.Pushbullet('o.quhdx5s5CTWKzG7EW9GJtOXXIuQCjYZk')
    pb.push_note(title, body)


def certificate_complete(df):
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
    result.drop(columns=['Certificate'], inplace=True)
    result['State'] = result['State'].replace('', pd.NA).fillna(result['City'])
    print('Checking for any Empty records...')

    result.to_csv(f'C:\\Users\\marie\\Documents\\Intoglo Office Documents\\Fieo Scraping\\completed\\Fieo_data_v3.csv', index = False)

    c_finish_time = time.time()
    print ("Csv is exported")
    print(f"Total time taken: {c_finish_time - start_time / 3600} hours")


#Main Code
start_time = time.time()
list_ = [x for x in range(1,101)]  # Your priority list
urls = url_generator(list_)
full_data = []
address_data = []
for url in urls:
    desired_headers, table_data = priority_scrapper(url)
    full_data.extend(table_data)
    print(url)
    break


# Convert to DataFrame
df = pd.read_csv('main_table_v3.csv') 
# df = df.head(50)
end_time = time.time()
print('Shape of Complete dataframe: ', df.shape)
print (f'Time Taken: {end_time - start_time} seconds')

#Certificate Processing
certificate_complete(df)

#Send Notification
send_notification("Fieo Data Scraper", "The Scraping is completed")
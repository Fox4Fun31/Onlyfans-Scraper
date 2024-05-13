import os
import requests
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
import os
from bs4 import BeautifulSoup

base_folder = "" # "C:\\Users\\Test", "root/Test/save"
process_single_entry = False  # Setze diese auf True, um nur einen Eintrag zu verarbeiten

wait = 5  # Wartezeit in Sekunden
tries = 100  # Anzahl der Wiederholungsversuche
maxgb = 400

def check_file_exists(file_path):
    return os.path.exists(file_path)


def get_existing_files(user_folder):
    bilder_folder = os.path.join(user_folder, "Bilder")
    video_folder = os.path.join(user_folder, "Videos")
    existing_files = {file for folder in [bilder_folder, video_folder] for file in os.listdir(folder)}
    return existing_files


def download(url, user_folder, wait, tries, current, total):
    bilder_folder = os.path.join(user_folder, "Bilder")
    video_folder = os.path.join(user_folder, "Videos")
    retries = tries
    while retries > 0:
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                filename = url.split('/')[-1]
                if any(url.endswith(ext) for ext in ['jpg', 'jpeg', 'png', 'gif']):
                    save_path = os.path.join(bilder_folder, filename)
                elif any(url.endswith(ext) for ext in ['mp4', 'avi', 'mov', 'm4v']):
                    save_path = os.path.join(video_folder, filename)
                else:
                    return

                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                print(f"Downloaded and saved {url} to {save_path} ({current} of {total})")
                break
            elif response.status_code == 429:
                print(f"Too many requests, waiting {wait} seconds...")
                time.sleep(wait)
                retries -= 1
            else:
                print(f"Failed to download {url}, status code {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
            break


def call_download(user_folder, wait, tries, post_distance):
    urls = get_urls_from_file(user_folder)
    existing_files = get_existing_files(user_folder)

    # Bestimme, welche URLs heruntergeladen werden müssen
    urls_to_download = [url for url in urls if url.split('/')[-1] not in existing_files]

    # Verwende post_distance, um zu bestimmen, wie viele URLs heruntergeladen werden sollen
    if post_distance is not None:
        urls_to_download = urls_to_download[:post_distance]  # Begrenzt die Liste auf die ersten 'post_distance' URLs
        print(f"URLs zur Download-Vorbereitung: {len(urls_to_download)} URLs basierend auf post_distance")

    if not urls_to_download:
        print("Keine neuen Dateien zum Downloaden vorhanden.")
        return

    total_urls = len(urls_to_download)
    print(f"Starte den Download von {total_urls} URLs.")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for i, url in enumerate(urls_to_download):
            print(f"Download-Thread startet für {url}")
            futures.append(executor.submit(download, url, user_folder, wait, tries, i+1, total_urls))
        for future in futures:
            future.result()

    print("Alle Downloads abgeschlossen.")
    create_completion_file(user_folder)

def get_urls_from_file(user_folder):
    urls_data_urls_path = os.path.join(user_folder, "urls_data_urls.txt")
    with open(urls_data_urls_path, 'r') as file:
        urls = [line.strip() for line in file.readlines()]
    return urls


def get_folder_size(base_folder):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(base_folder):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total_size += os.path.getsize(fp)
    return total_size / (1024 ** 3)  # Größe in GB



def rly_download(user_folder, wait, tries, maxgb):
    fertig_file = os.path.join(user_folder, "fertig.txt")
    if not os.path.exists(fertig_file):
        print("Fertig.txt existiert nicht, starte vollständigen Download")
        call_download(user_folder, wait, tries, None)  # Keine spezifische Begrenzung, wenn keine fertig.txt existiert
    else:
        post_distance = post_urls_fertig_distance_calc(user_folder)
        print(f"Berechnete Post-Distance: {post_distance}")

        if post_distance <= 0:
            print("Keine neuen Dateien zu downloaden oder Fehler in der Post-Distance Berechnung.")
            return

        current_size = get_folder_size(user_folder)
        print(f"Aktuelle Größe: {current_size:.2f} GB, maximal erlaubt: {maxgb} GB")

        if current_size < maxgb:
            print("Ausreichend Speicherplatz vorhanden, starte Download...")
            call_download(user_folder, wait, tries, post_distance)  # Verwende post_distance korrekt
        else:
            print("Nicht genug Speicherplatz vorhanden.")







def post_urls_fertig_distance_calc(user_folder):
    urls_data_urls_path = os.path.join(user_folder, "urls_data_urls.txt")
    fertig_file = os.path.join(user_folder, "fertig.txt")
    
    try:
        with open(urls_data_urls_path, 'r') as f:
            post_urls = [url.strip() for url in f.readlines()]
        with open(fertig_file, 'r') as f:
            fertig_url = f.readlines()[1].strip()  # Zweite Zeile lesen und trimmen
        print(f"Anzahl der URLs: {len(post_urls)}")
        print(f"Suche nach URL: {fertig_url}")
    except FileNotFoundError as e:
        print(f"Fehler beim Öffnen der Dateien: {e}")
        return rly_download(user_folder, wait, tries, maxgb)

    if fertig_url in post_urls:
        post_distance = post_urls.index(fertig_url)
        print(f"URL gefunden an Position: {post_distance}")
    else:
        print("URL nicht in der Liste gefunden.")
        post_distance = -1  # Kann durch eine andere Handhabung ersetzt werden, z.B. einen Fehler werfen

    return post_distance
   


def create_directories(base_folder, user_name):
    neu_folder = os.path.join(base_folder, "neu")
    if not os.path.exists(neu_folder):
        os.makedirs(neu_folder)
        print(f"Hauptordner erstellt: {neu_folder}")

    user_folder = os.path.join(neu_folder, user_name)
    bilder_folder = os.path.join(user_folder, "Bilder")
    video_folder = os.path.join(user_folder, "Videos")
    for folder in [user_folder, bilder_folder, video_folder]:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Ordner erstellt: {folder}")
    return user_folder

def create_completion_file(user_folder):
    fertig_path = os.path.join(user_folder, "fertig.txt")
    urls_data_urls_path = os.path.join(user_folder, "urls_data_urls.txt")
    
    # Lesen der ersten Zeile aus der post_urls.txt
    try:
        with open(urls_data_urls_path, 'r') as f:
            first_line = f.readline().strip()
    except FileNotFoundError:
        first_line = "Post URLs file not found"
    
    # Schreiben in die fertig.txt Datei
    with open(fertig_path, 'w') as f:
        f.write(f"{base_folder + user_folder}\n")
        f.write(first_line + "\n")

    print(f"Completion file created at {fertig_path}")


def extract_name_from_url(url):
    parts = url.split('/')
    name_index = parts.index('user') + 1
    return parts[name_index] if name_index < len(parts) else None

def fetch_post_anzahl(baseurl):
    retries = tries  # Maximale Anzahl von Wiederholungsversuchen
    while retries > 0:
        try:
            response = requests.get(baseurl)
            response.raise_for_status()  # Dies wirft eine Exception bei 4xx und 5xx Antworten
            html_content = response.text
            match = re.search(r'Showing \d+ - \d+ of (\d+)', html_content)
            return int(match.group(1)) if match else html_content.count('<article')
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"Zu viele Anfragen, warte {wait} Sekunden...")
                time.sleep(wait)
                retries -= 1
            else:
                raise  # Andere HTTP-Fehler erneut auslösen
        except requests.exceptions.RequestException as e:
            print(f"Ein Fehler ist aufgetreten beim Abrufen der URL {baseurl}: {e}")
            return 0

def fetch_posts(baseurl, user_folder):
    total_posts = fetch_post_anzahl(baseurl)
    posts = []
    offset = 0
    api_url = baseurl.replace("https://coomer.su/", "https://coomer.su/api/v1/")
    
    while offset < total_posts:
        retries = tries  # Maximale Anzahl von Wiederholungsversuchen
        while retries > 0:
            try:
                paginated_url = f"{api_url}?o={offset}"
                response = requests.get(paginated_url)
                response.raise_for_status()
                page_posts = response.json()
                posts.extend(page_posts)
                offset += 50
                break  # Erfolgreich, breche die innere Schleife ab
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    print(f"Zu viele Anfragen, warte {wait} Sekunden...")
                    time.sleep(wait)
                    retries -= 1
                else:
                    raise
            except requests.exceptions.RequestException as e:
                print(f"Fehler beim Abrufen von Posts für {paginated_url}: {e}")
                break

    with open(os.path.join(user_folder, "posts_response.json"), "w") as json_file:
        json.dump(posts, json_file, indent=4)
    
    fetch_all_creator_urls(user_folder)

    return posts

def withaccornot():
    response = input("Sollen die Fav neu vom Acc gefetched werden? (y/n): ")
    if response.lower() == 'y':
        fetch_fav()
        return True
    elif response.lower() == 'n':
        return False
    else:
        print("Ungültige Eingabe. Bitte nur 'y' oder 'n' eingeben.")
        return withaccornot()

def cleaning():
    input_file_path = 'urls.txt'
    output_file_path = 'urls.txt'
    
    with open(input_file_path, 'r') as file:
        lines = file.readlines()

    with open(output_file_path, 'w') as outfile:
        for line in lines:
            clean_line = line.strip()
            if not clean_line.startswith('https://coomer.su'):
                clean_line = f'https://coomer.su/onlyfans/user/{clean_line}'
            outfile.write(clean_line + '\n')



def save_post_urls(posts, user_folder, baseurl):
    post_urls_file = os.path.join(user_folder, "post_urls.txt")
    with open(post_urls_file, "w") as urls_file:
        total = len(posts)
        for i, post in enumerate(posts):
            post_url = f"{baseurl}/post/{post['id']}"
            if i < total - 1:
                urls_file.write(post_url + "\n")
            else:
                urls_file.write(post_url)  # Letzte URL ohne Zeilenumbruch


def scraper(baseurl, name):
    if name:
        user_folder = create_directories(base_folder, name)
        print(f"Benutzerverzeichnis für '{name}' erstellt: {user_folder}")
        posts = fetch_posts(baseurl, user_folder)
        save_post_urls(posts, user_folder, baseurl)
        rly_download(user_folder, wait, tries, maxgb)
    else:
        print("Kein gültiger Name gefunden.")

def fetch_all_creator_urls(user_folder):
    posts_json_path = os.path.join(user_folder, "posts_response.json")
    urls_data_urls_path = os.path.join(user_folder, "urls_data_urls.txt")

    try:
        with open(posts_json_path, 'r') as file:
            posts_data = json.load(file)

        attachments = []
        for post in posts_data:
            if 'file' in post and 'path' in post['file']:
                attachment_url = "https://c1.coomer.su/data" + post['file']['path']
                attachments.append(attachment_url)
            if 'attachments' in post:
                for attachment in post['attachments']:
                    if 'path' in attachment:
                        attachment_url = "https://c1.coomer.su/data" + attachment['path']
                        attachments.append(attachment_url)

        # Vorhandene URLs aus der Datei lesen
        existing_urls = set()
        if os.path.exists(urls_data_urls_path):
            with open(urls_data_urls_path, 'r') as urls_file:
                existing_urls = set(urls_file.read().splitlines())

        # Neue URLs hinzufügen, nur wenn sie nicht bereits vorhanden sind
        with open(urls_data_urls_path, 'a') as urls_file:
            for attachment_url in attachments:
                if attachment_url not in existing_urls:
                    urls_file.write(attachment_url + "\n")
                    existing_urls.add(attachment_url)
                    print(f"Attachment URL wurde hinzugefügt: {attachment_url}")
                else:
                    print(f"Die Attachment URL existiert bereits: {attachment_url}")

    except FileNotFoundError:
        print(f"Datei {posts_json_path} wurde nicht gefunden.")
    except json.JSONDecodeError:
        print("Fehler beim Parsen der JSON-Daten in 'posts_response.json'.")


def fetch_fav():
    def clear_artist_urls_file():
        file_path = "urls.txt"
        if os.path.exists(file_path):
            os.remove(file_path)

    clear_artist_urls_file()
    
    def get_favorite_artists():
        url = "https://coomer.su/api/v1/account/favorites?type=artist"
        headers = {
            "accept": "application/json",
            "Cookie": "session=..." # session key from coomer.su
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to retrieve favorite artists.")
            return None

    def sort_favorite_artists_by_faved_seq_desc(artists):
        return sorted(artists, key=lambda x: x["faved_seq"], reverse=True)

    def save_urls(artists):
        for artist in artists:
            if artist["service"] == "fansly":
                url = f"https://coomer.su/fansly/user/{artist['id']}"
                save_url(url)
            elif artist["service"] == "onlyfans":
                url = f"https://coomer.su/onlyfans/user/{artist['name']}"
                save_url(url)

    def save_url(url):
        with open("urls.txt", "a") as file:
            file.write(url + "\n")

    favorite_artists = get_favorite_artists()

    if favorite_artists:
        sorted_artists = sort_favorite_artists_by_faved_seq_desc(favorite_artists)
        save_urls(sorted_artists)



def main():
    if withaccornot():
        cleaning()
        with open('urls.txt', 'r') as file:
            for line in file:
                line = line.strip()
                name = extract_name_from_url(line)
                baseurl = line
                print(f"Base URL: {baseurl}")
                scraper(baseurl, name)
                if process_single_entry:
                    break
    else:
        cleaning()
        with open('urls.txt', 'r') as file:
            for line in file:
                line = line.strip()
                name = extract_name_from_url(line)
                baseurl = line
                print(f"Base URL: {baseurl}")
                scraper(baseurl, name)
                if process_single_entry:
                    break

if __name__ == "__main__":
    main()

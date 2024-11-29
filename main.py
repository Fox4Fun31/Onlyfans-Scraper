import os
import requests
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
import threading

# Define your base folder and other constants
base_folder = "yoursavepath"
urls_path = "yourpath/urls.txt"
key = "session=yourKey"
process_single_entry = False  # Set this to True to process only one entry

wait = 5  # Wait time in seconds
tries = 100  # Number of retries
maxgb = 470
timeout = 7200  # 12 hours in seconds

download_lock = threading.Lock()  # Lock for thread-safe writing

def check_file_exists(file_path):
    return os.path.exists(file_path)

def get_existing_files(user_folder):
    bilder_folder = os.path.join(user_folder, "Bilder")
    video_folder = os.path.join(user_folder, "Videos")
    audio_folder = os.path.join(user_folder, "Audio")
    existing_files = {file for folder in [bilder_folder, video_folder, audio_folder] for file in os.listdir(folder)}
    return existing_files

def get_file_size(file_path):
    return os.path.getsize(file_path) if os.path.exists(file_path) else 0

def log_downloaded_url(url, log_file_path):
    normalized_url = re.sub(r'https://(c1|n1)\.coomer\.su/data', "https://coomer.su/data", url)
    with download_lock:
        with open(log_file_path, 'a') as log_file:
            log_file.write(normalized_url + '\n')

def download(url, save_path, wait, tries, current, total, log_file_path):
    retries = tries
    while retries > 0:
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                
                if os.path.exists(save_path):  # Überprüfen, ob die Datei existiert
                    actual_size = os.path.getsize(save_path)
                    expected_size = int(response.headers.get('content-length', 0))
                    if actual_size == expected_size:
                        print(f"Downloaded and saved {url} to {save_path} ({current} of {total})")
                        log_downloaded_url(url, log_file_path)
                        return True
                    else:
                        print(f"Incomplete download detected for {url}. Expected size: {expected_size}, actual size: {actual_size}")
                        os.remove(save_path)
                        return False
                else:
                    print(f"File {save_path} does not exist after download attempt.")
                    return False
            elif response.status_code == 429:
                print(f"Too many requests, waiting {wait} seconds...")
                time.sleep(wait)
                retries -= 1
            else:
                print(f"Failed to download {url}, status code {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
            return False
    return False


def call_download(user_folder, wait, tries, urls_to_download):
    if not urls_to_download:
        print("Keine neuen Dateien zum Downloaden vorhanden.")
        return

    total_urls = len(urls_to_download)
    print(f"Starte den Download von {total_urls} URLs.")

    existing_files = get_existing_files(user_folder)
    bilder_folder = os.path.join(user_folder, "Bilder")
    video_folder = os.path.join(user_folder, "Videos")
    audio_folder = os.path.join(user_folder, "Audio")
    urls_to_really_download = []

    log_file_path = os.path.join(user_folder, 'downloaded.txt')

    for url in urls_to_download:
        filename = url.split('/')[-1]
        if any(url.endswith(ext) for ext in ['jpg', 'jpeg', 'png', 'gif']):
            save_path = os.path.join(bilder_folder, filename)
        elif any(url.endswith(ext) for ext in ['mp4', 'avi', 'mov', 'm4v']):
            save_path = os.path.join(video_folder, filename)
        elif any(url.endswith(ext) for ext in ['mp3', 'wav', 'flac']):
            save_path = os.path.join(audio_folder, filename)
        else:
            continue

        normalized_url = re.sub(r'https://(c1|n1)\.coomer\.su/data', "https://coomer.su/data", url)
        if filename in existing_files and get_file_size(save_path) == int(requests.head(url).headers.get('content-length', 0)):
            print(f"File {filename} already exists and is complete.")
            log_downloaded_url(normalized_url, log_file_path)
        else:
            urls_to_really_download.append(url)

    print(f"URLs to really download: {len(urls_to_really_download)}")

    if not urls_to_really_download:
        print("Keine neuen Dateien zum Downloaden vorhanden.")
        return

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for i, url in enumerate(urls_to_really_download):
            filename = url.split('/')[-1]
            if any(url.endswith(ext) for ext in ['jpg', 'jpeg', 'png', 'gif']):
                save_path = os.path.join(bilder_folder, filename)
            elif any(url.endswith(ext) for ext in ['mp4', 'avi', 'mov', 'm4v']):
                save_path = os.path.join(video_folder, filename)
            elif any(url.endswith(ext) for ext in ['mp3', 'wav', 'flac']):
                save_path = os.path.join(audio_folder, filename)
            else:
                continue

            print(f"Start downloading: {url}")
            futures.append(executor.submit(download, url, save_path, wait, tries, i+1, total_urls, log_file_path))
        for future in futures:
            future.result()  # We don't need to handle the result here

    print("Alle Downloads abgeschlossen.")
    create_completion_file(user_folder)

def get_urls_from_file(file_path):
    if not os.path.exists(file_path):
        return set()
    with open(file_path, 'r') as file:
        return set(re.sub(r'https://(c1|n1)\.coomer\.su/data', "https://coomer.su/data", line.strip()) for line in file)

def save_urls_to_file(file_path, urls):
    with open(file_path, 'w') as file:
        for url in sorted(urls):  # Optional: sort the URLs for consistency
            file.write(url + '\n')

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
    urls_data_urls_path = os.path.join(user_folder, "urls_data_urls.txt")
    downloaded_urls_path = os.path.join(user_folder, "downloaded.txt")

    while True:
        if os.path.exists(fertig_file):
            print("Fertig.txt existiert, überprüfe heruntergeladene Dateien.")
            
            urls_data_urls = get_urls_from_file(urls_data_urls_path)
            downloaded_urls = get_urls_from_file(downloaded_urls_path)
            
            urls_to_download = list(urls_data_urls - downloaded_urls)

            print(f"Berechne fehlende Dateien: {len(urls_to_download)} URLs fehlen.")

            current_size = get_folder_size(base_folder)
            print(f"Aktuelle Größe: {current_size:.2f} GB, maximal erlaubt: {maxgb} GB")

            if current_size < maxgb:
                print("Ausreichend Speicherplatz vorhanden, starte Download...")
                call_download(user_folder, wait, tries, urls_to_download)  # Starte den Download der fehlenden URLs
                break
            else:
                print("Nicht genug Speicherplatz vorhanden. Warte 12 Stunden...")
                time.sleep(timeout)
        else:
            print("Fertig.txt existiert nicht, starte vollständigen Download")
            urls_data_urls = get_urls_from_file(urls_data_urls_path)
            downloaded_urls = get_urls_from_file(downloaded_urls_path)
            urls_to_download = list(urls_data_urls - downloaded_urls)
            call_download(user_folder, wait, tries, urls_to_download)  # Volle Liste zum Download
            break

def create_directories(base_folder, user_name):
    neu_folder = os.path.join(base_folder, "neu")
    if not os.path.exists(neu_folder):
        os.makedirs(neu_folder)
        print(f"Hauptordner erstellt: {neu_folder}")

    user_folder = os.path.join(neu_folder, user_name)
    bilder_folder = os.path.join(user_folder, "Bilder")
    video_folder = os.path.join(user_folder, "Videos")
    audio_folder = os.path.join(user_folder, "Audio")
    for folder in [user_folder, bilder_folder, video_folder, audio_folder]:
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

import requests

def fetch_post_anzahl(baseurl):
    api_url = baseurl.replace("https://coomer.su/", "https://coomer.su/api/v1/")
    offset = 0
    wait = 1  # Wartezeit zwischen den Anfragen, falls gewünscht

    while True:
        paginated_url = f"{api_url}?o={offset}"
        try:
            response = requests.get(paginated_url, timeout=4)  # Timeout auf 4 Sekunden gesetzt
            response.raise_for_status()

            page_posts = response.json()

            # Wenn keine Posts mehr kommen (leere Liste oder Fehlermeldung), breche die Schleife ab
            if not page_posts:
                print(f"Keine weiteren Posts ab Offset {offset}, Gesamtposts: {offset}")
                return offset  # Rückgabe der Gesamtzahl der Posts

            # Wenn mehr Posts zurückgegeben werden, erhöhe das Offset um 50
            offset += 50

        except requests.exceptions.HTTPError as e:
            print(f"HTTPError bei {paginated_url}: {e.response.status_code}")
            break  # Beende die Schleife bei einem HTTP-Fehler (optional)
        except requests.exceptions.RequestException as e:
            print(f"Fehler bei der Anfrage: {e}")
            break  # Beende die Schleife bei einem allgemeinen Fehler (optional)

    return 0  # Falls die Schleife aus irgendeinem Grund abbricht


def fetch_posts(baseurl, user_folder):
    total_posts = fetch_post_anzahl(baseurl)
    posts = []
    offset = 0
    api_url = baseurl.replace("https://coomer.su/", "https://coomer.su/api/v1/")
    print(api_url, "angefragte apiurl")
    while offset < total_posts:
        retries = tries  # Maximale Anzahl von Wiederholungsversuchen
        while retries > 0:
            try:
                paginated_url = f"{api_url}?o={offset}"
                response = requests.get(paginated_url, timeout=4)  # Timeout auf 4 Sekunden gesetzt
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
                elif e.response.status_code == 503:
                    print(f"Serverfehler (503) bei {paginated_url}, warte {wait} Sekunden...")
                    time.sleep(wait)
                    retries -= 1
                elif e.response.status_code == 403:
                    print(f"Zugriff verweigert (403) bei {paginated_url}, warte {wait} Sekunden...")
                    time.sleep(wait)
                    retries -= 1
                else:
                    raise
            except requests.exceptions.ConnectTimeout:
                print(f"Zeitüberschreitung bei der Verbindung zu {paginated_url}, versuche erneut...")
                time.sleep(wait)
                retries -= 1
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
        existing_urls = get_urls_from_file(urls_data_urls_path)

        # Neue URLs hinzufügen
        new_urls = set(attachments)

        # Kombiniere bestehende und neue URLs, um Duplikate zu vermeiden
        all_urls = existing_urls.union(new_urls)

        # Aktualisierte URLs in die Datei schreiben
        save_urls_to_file(urls_data_urls_path, all_urls)

        print(f"Datei {urls_data_urls_path} wurde aktualisiert.")

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
            "Cookie": key
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
            elif artist["service"] == "candfans":
                url = f"https://coomer.su/candfans/user/{artist['name']}"
                save_url(url)

    def save_url(url):
        with open(urls_path, "a") as file:
            file.write(url + "\n")

    favorite_artists = get_favorite_artists()

    if favorite_artists:
        sorted_artists = sort_favorite_artists_by_faved_seq_desc(favorite_artists)
        save_urls(sorted_artists)

def main():
    if withaccornot():
        cleaning()
        with open(urls_path, 'r') as file:
            for line in file:
                current_size = get_folder_size(base_folder)
                if current_size >= maxgb:
                    print(f"Current size {current_size:.2f} GB exceeds max allowed {maxgb} GB. Waiting for 12 hours...")
                    time.sleep(timeout)
                    continue

                line = line.strip()
                name = extract_name_from_url(line)
                baseurl = line
                print(f"Base URL: {baseurl}")
                scraper(baseurl, name)
                if process_single_entry:
                    break
    else:
        cleaning()
        with open(urls_path, 'r') as file:
            for line in file:
                current_size = get_folder_size(base_folder)
                if current_size >= maxgb:
                    print(f"Current size {current_size:.2f} GB exceeds max allowed {maxgb} GB. Waiting for 12 hours...")
                    time.sleep(timeout)
                    continue

                line = line.strip()
                name = extract_name_from_url(line)
                baseurl = line
                print(f"Base URL: {baseurl}")
                scraper(baseurl, name)
                if process_single_entry:
                    break

if __name__ == "__main__":
    main()

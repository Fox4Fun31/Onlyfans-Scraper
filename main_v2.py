import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urlparse, parse_qs, urljoin
from collections import OrderedDict
import threading
import time
import queue

# Global variable for the maximum folder size before pausing downloads
max_folder_size_gb = 380

def countdown(t):
    while t:
        mins, secs = divmod(t, 60)
        hours, mins = divmod(mins, 60)
        timeformat = '{:02d}:{:02d}:{:02d}'.format(hours, mins, secs)
        print(timeformat, end='\r')
        time.sleep(1)
        t -= 1


def get_folder_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total_size += os.path.getsize(fp)
    return total_size / (1024 ** 3)  # Größe in GB


def extract_media_links(soup):
    links = []
    for thumb in soup.select('.post__thumbnail a.fileThumb'):
        links.append(thumb['href'])
    for video_link in soup.select('.post__attachments a.post__attachment-link'):
        links.append(video_link['href'])
    return links


def get_total_post_count(base_url):
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        post_count_text = soup.select_one('small')
        if post_count_text and 'of' in post_count_text.text:
            total_post_count = int(post_count_text.text.split('of')[1].strip())
        else:
            total_post_count = len(soup.select('.post-card.post-card--preview'))
        return total_post_count
    except requests.RequestException as e:
        print(f"Fehler beim Abrufen der Gesamtanzahl der Posts von {base_url}: {e}")
        return 0


def extract_post_links_threaded(base_url, offset, total_post_count, output_queue):
    paginated_url = f"{base_url}?o={offset}" if offset > 0 else base_url
    try:
        response = requests.get(paginated_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        page_links = [urljoin(base_url, a['href']) for a in soup.select('.post-card.post-card--preview a')]
        output_queue.put((offset, page_links))
    except requests.RequestException as e:
        print(f"Fehler beim Abrufen der Post-Links von {paginated_url}: {e}")


def extract_post_links(base_url, total_post_count):
    post_links = []

    for offset in range(0, total_post_count, 50):
        paginated_url = f"{base_url}?o={offset}" if offset > 0 else base_url
        try:
            response = requests.get(paginated_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            page_links = [urljoin(base_url, a['href']) for a in soup.select('.post-card.post-card--preview a')]
            post_links.extend(page_links)
        except requests.RequestException as e:
            print(f"Fehler beim Abrufen der Post-Links von {paginated_url}: {e}")

    return post_links


def fetch_post_data(post_url, total_post_count, post_index, base_folder):
    max_attempts = 500
    attempt = 0
    while attempt < max_attempts:
        try:
            print(f"Fetching Post {post_index} von {total_post_count}: {post_url}")
            response = requests.get(post_url)
            response.raise_for_status()
            if get_folder_size(base_folder) >= max_folder_size_gb:
                print("Ordnergröße von 280 GB überschritten, Beenden des Downloads.")
                return ""
            soup = BeautifulSoup(response.content, 'html.parser')
            media_links = extract_media_links(soup)
            post_data = f"{post_index} von {total_post_count}\n{post_url}\n" + "\n".join(
                ['\t' + link for link in media_links]) + "\n"
            print(f"Post {post_index} verarbeitet.")
            return post_data
        except requests.RequestException as e:
            print(f"Fehler beim Abrufen von {post_url}: {e}. Versuch {attempt + 1} von {max_attempts}")
            time.sleep(60)
            attempt += 1
    return ""


def create_directories(base_folder, user_name):
    user_folder = os.path.join(base_folder, user_name)
    bilder_folder = os.path.join(user_folder, "Bilder")
    video_folder = os.path.join(user_folder, "Videos")
    for folder in [user_folder, bilder_folder, video_folder]:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Ordner erstellt: {folder}")
    return user_folder


def countdown(t):
    while t:
        mins, secs = divmod(t, 60)
        hours, mins = divmod(mins, 60)
        timeformat = '{:02d}:{:02d}:{:02d}'.format(hours, mins, secs)
        print(timeformat, end='\r')
        time.sleep(1)
        t -= 1


def download_file(url, file_path, chunk_size=1048576, base_folder='/root/Python/sachen', post_index=0, total_posts=0,
                  attachment_index=1, total_attachments=1):
    start_time = time.time()
    max_retries = 50000
    retry_delay = 60
    attempt = 0

    while attempt < max_retries:
        try:
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        # Überprüfung der Ordnergröße während des Downloads
                        if get_folder_size(base_folder) >= max_folder_size_gb:
                            print("Ordnergröße überschritten, pausiere Download.")
                            countdown(3600)  # 1 Stunde Wartezeit
                            # Nach der Wartezeit, überprüfe die Ordnergröße erneut
                            continue

                        file.write(chunk)
                        progress = file.tell() / total_size * 100
                        bar_length = 30
                        num_bars = int(progress / (100 / bar_length))
                        progress_bar = "=" * num_bars + ">" + "." * (bar_length - num_bars)
                        speed = file.tell() / (time.time() - start_time) / 1000000
                        print(
                            f"\rDownload Fortschritt für Anhang {attachment_index} von Post {post_index} von {total_posts}: [{progress_bar}] {progress:.1f}% ({speed:.2f} MBPS)",
                            end="")
                        time.sleep(0.1)
                print("\nDownload abgeschlossen!")
                return
        except requests.RequestException as e:
            print(
                f"Verbindungsfehler: Versuch {attempt + 1} von {max_retries}. Warte {retry_delay} Sekunden. Fehler: {e}")
            time.sleep(retry_delay)
            attempt += 1


def check_file_exists(file_path):
    return os.path.exists(file_path)


def download_from_links_file(all_links_path, base_folder, total_posts, fertig_file_path, current_first_post):
    with open(all_links_path, 'r') as file:
        lines = file.readlines()

    post_index = 1
    attachment_index = 1
    for line in lines:
        line_data = line.strip()
        if line_data.startswith("https://"):
            url = line_data
            file_path = generate_file_path_from_url(base_folder, url)

            if file_path and not check_file_exists(file_path):
                download_file(url, file_path, base_folder=base_folder, post_index=post_index, total_posts=total_posts,
                              attachment_index=attachment_index, total_attachments=len(lines))
                attachment_index += 1
        else:
            # Dies setzt den Anhang-Index zurück, wenn ein neuer Post beginnt
            attachment_index = 1
            post_index += 1

    # Erstellen der fertig.txt Datei nach erfolgreichem Download aller Dateien
    with open(fertig_file_path, 'w') as fertig_file:
        fertig_file.write(f"{base_folder}\n{current_first_post}")

    print("Alle Dateien wurden erfolgreich heruntergeladen und fertig.txt wurde erstellt.")


def scrape_creator(base_url, base_folder):
    user_name = base_url.split('/')[-1]
    user_folder = create_directories(base_folder, user_name)
    fertig_file_path = os.path.join(user_folder, "fertig.txt")

    # Prüfung, ob fertig.txt existiert und ob der erste Post gleich ist
    if check_fertig_file_and_first_post(base_url, fertig_file_path):
        print(f"Keine neuen Posts für {user_name}. Überspringe den Creator.")
        return

    all_links_path = os.path.join(user_folder, "all_links.txt")
    total_post_count = get_total_post_count(base_url)
    post_links = extract_post_links(base_url, total_post_count)

    post_data_dict = OrderedDict()

    for i, post_url in enumerate(post_links, 1):
        print(f"Fetching Post {i} von {total_post_count}: {post_url}")
        post_data = fetch_post_data(post_url, total_post_count, i, user_folder)
        if post_data:
            post_data_dict[i] = post_data

    with open(all_links_path, 'w') as f:
        for i in sorted(post_data_dict.keys()):
            f.write(post_data_dict[i])

    download_from_links_file(all_links_path, user_folder, total_post_count, fertig_file_path,
                             get_current_first_post_url(base_url))


def check_fertig_file_and_first_post(base_url, fertig_file_path):
    if os.path.exists(fertig_file_path):
        with open(fertig_file_path, 'r') as file:
            lines = file.readlines()
            if len(lines) > 1:
                saved_url = lines[1].strip()
                current_first_post_url = get_current_first_post_url(base_url)
                return saved_url == current_first_post_url
    return False


def get_last_first_post_url(fertig_file_path):
    if os.path.exists(fertig_file_path):
        with open(fertig_file_path, 'r') as fertig_file:
            lines = fertig_file.readlines()
            return lines[1].strip() if len(lines) > 1 else None
    return None


def update_post_links(last_first_post_url, post_links):
    try:
        last_first_post_index = post_links.index(last_first_post_url)
        return post_links[last_first_post_index:]
    except ValueError:
        return post_links


def download_missing_files(all_links_path, base_folder):
    with open(all_links_path, 'r') as file:
        lines = file.readlines()

    post_index = 1
    attachment_index = 1
    total_missing_files = 0

    # Zählen der gesamten fehlenden Dateien
    for line in lines:
        if line.strip().startswith("https://"):
            file_path = generate_file_path_from_url(base_folder, line.strip())
            if file_path and not check_file_exists(file_path):
                total_missing_files += 1
        else:
            post_index += 1

    # Zurücksetzen der Zähler für den Download-Prozess
    post_index = 1
    attachment_index = 1

    # Durchgehen der Liste erneut, um die fehlenden Dateien herunterzuladen
    for line in lines:
        line_data = line.strip()
        if line_data.startswith("https://"):
            file_path = generate_file_path_from_url(base_folder, line_data)
            if file_path and not check_file_exists(file_path):
                # Überprüfung der Ordnergröße vor dem Download
                if get_folder_size(base_folder) >= max_folder_size_gb:
                    print("Ordnergröße überschritten, pausiere Download.")
                    wait_time = 3600  # 1 Stunde Wartezeit
                    countdown(wait_time)
                    print("Fortsetzung des Downloads...")

                download_file(line_data, file_path, post_index=post_index, total_posts=post_index,
                              attachment_index=attachment_index, total_attachments=total_missing_files)
                attachment_index += 1
        else:
            post_index += 1
            attachment_index = 1

    print("Alle fehlenden Dateien wurden erfolgreich heruntergeladen.")


def generate_file_path_from_url(base_folder, url):
    parsed_url = urlparse(url)
    file_name = os.path.basename(parsed_url.path)
    query_params = parse_qs(parsed_url.query)
    if 'f' in query_params:
        file_name = query_params['f'][0]
    return generate_file_path(base_folder, file_name)


def generate_file_path(base_folder, file_name):
    lower_file_name = file_name.lower()
    if any(lower_file_name.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif']):
        return os.path.join(base_folder, 'Bilder', file_name)
    elif any(lower_file_name.endswith(ext) for ext in ['.mp4', '.avi', '.mov', '.m4v']):
        return os.path.join(base_folder, 'Videos', file_name)
    else:
        print("Unbekannter Dateityp: ", file_name)
        return None


def check_file_exists(file_path):
    return os.path.exists(file_path)


def check_and_update_links_file(base_url, all_links_path):
    # Überprüfe, ob die Datei existiert
    if os.path.exists(all_links_path):
        with open(all_links_path, 'r') as file:
            first_line = file.readline().strip()
            first_url_in_file = first_line.split('\n')[0].split(' ')[1] if first_line else ''

        # Holen Sie sich den aktuellen ersten Post von der Webseite
        current_first_post_url = get_current_first_post_url(base_url)

        # Vergleiche URLs. Wenn sie übereinstimmen, überspringe das Scraping
        if current_first_post_url == first_url_in_file:
            print("Keine neuen Posts gefunden, überspringe das Scraping für", base_url)
            return False
    else:
        # Datei existiert nicht, erstelle sie
        open(all_links_path, 'w').close()

    # Führe das Scraping durch, da entweder die Datei nicht existiert oder der erste Post unterschiedlich ist
    return True


def get_current_first_post_url(base_url):
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        first_post = soup.select_one('.post-card.post-card--preview a')
        if first_post:
            return urljoin(base_url, first_post['href'])
    except requests.RequestException as e:
        print(f"Fehler beim Abrufen des ersten Posts von {base_url}: {e}")
    return ''


def update_and_download_new_posts(base_url, base_folder, all_links_path, fertig_file_path):
    # Schritt 1: Holen der aktuellen Post-Links
    total_post_count = get_total_post_count(base_url)
    current_post_links = extract_post_links(base_url, total_post_count)

    # Schritt 2: Bestimmen der neuen Posts
    last_first_post_url = get_last_first_post_url(fertig_file_path)
    new_post_links = update_post_links(last_first_post_url, current_post_links)

    # Schritt 3: Download der neuen Posts
    if new_post_links:
        print(f"Es gibt {len(new_post_links)} neue Posts zum Herunterladen.")
        user_name = base_url.split('/')[-1]
        user_folder = create_directories(base_folder, user_name)
        post_data_dict = OrderedDict()

        for i, post_url in enumerate(new_post_links, 1):
            print(f"Fetching Post {i} von {len(new_post_links)}: {post_url}")
            post_data = fetch_post_data(post_url, len(new_post_links), i, user_folder)
            if post_data:
                post_data_dict[i] = post_data

        with open(all_links_path, 'a') as f:  # 'a' zum Anhängen der neuen Links
            for i in sorted(post_data_dict.keys()):
                f.write(post_data_dict[i])

        download_from_links_file(all_links_path, user_folder, len(new_post_links), fertig_file_path,
                                 current_post_links[0])
    else:
        print("Keine neuen Posts zum Herunterladen.")


def get_last_first_post_url(fertig_file_path):
    """
    Extrahiert die URL des ersten Posts aus der 'fertig.txt'.
    """
    try:
        with open(fertig_file_path, 'r') as file:
            lines = file.readlines()
            if len(lines) > 1:
                # Der erste Post URL befindet sich in der zweiten Zeile
                return lines[1].strip()
    except FileNotFoundError:
        return None


def update_post_links(last_first_post_url, current_post_links):
    """
    Bestimmt die neuen Posts, die seit dem letzten gespeicherten Post hinzugefügt wurden.
    """
    if last_first_post_url in current_post_links:
        last_index = current_post_links.index(last_first_post_url)
        return current_post_links[:last_index]  # Rückgabe der neuen Posts
    return current_post_links  # Wenn keine Übereinstimmung gefunden wurde, betrachte alle Links als neu


def main():
    base_folder = ("/root/Python/Sachen/neu")
    with open('urls.txt', 'r') as file:
        creators = [line.strip() for line in file.readlines()]
    for creator_input in creators:
        base_url = urljoin("https://coomer.su/onlyfans/user/", creator_input)
        scrape_creator(base_url, base_folder)


if __name__ == "__main__":
    main()
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def get_youtube_chapters(url):
    options = webdriver.ChromeOptions()

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        driver.get(url)
        time.sleep(8)
        driver.execute_script("window.scrollTo(0, 600);")
        time.sleep(3)

        wait = WebDriverWait(driver, 20)

        # Click "View all" if available
        try:
            view_all = driver.find_element(By.CSS_SELECTOR, "button[aria-label='View all']")
            view_all.click()
            time.sleep(2)
            print("✅ Clicked 'View all'")
        except:
            print("ℹ️  No 'View all' button")

        card_list = wait.until(EC.presence_of_element_located((
            By.TAG_NAME, "ytd-horizontal-card-list-renderer"
        )))

        shelf     = card_list.find_element(By.ID, "shelf-container")
        scroll    = shelf.find_element(By.ID, "scroll-container")
        items_div = scroll.find_element(By.ID, "items")

        chapter_elements = items_div.find_elements(
            By.TAG_NAME, "ytd-macro-markers-list-item-renderer"
        )
        print(f"✅ Found {len(chapter_elements)} chapter elements")

        chapters = []
        for item in chapter_elements:
            try:
                title = item.find_element(
                    By.CSS_SELECTOR, "h4.macro-markers"
                ).get_attribute("title")

                # ── Fix: try multiple ways to get timestamp ──────
                timestamp = ""

                # Method 1: .text on #time
                try:
                    t = item.find_element(By.ID, "time")
                    timestamp = t.text.strip()
                except:
                    pass

                # Method 2: innerText via JS if .text is empty
                if not timestamp:
                    try:
                        t = item.find_element(By.ID, "time")
                        timestamp = driver.execute_script(
                            "return arguments[0].innerText;", t
                        ).strip()
                    except:
                        pass

                # Method 3: extract from href  (?t=28s → 0:28)
                if not timestamp:
                    try:
                        href = item.find_element(
                            By.CSS_SELECTOR, "a#endpoint"
                        ).get_attribute("href")

                        if "t=" in href:
                            seconds = int(href.split("t=")[1].replace("s", "").split("&")[0])
                            mins, secs = divmod(seconds, 60)
                            timestamp = f"{mins}:{secs:02d}"
                    except:
                        pass

                chapters.append({"title": title, "timestamp": timestamp})

            except Exception as e:
                print(f"  ⚠️ Skipped item: {e}")
                continue

        return {
            "url":           url,
            "chapter_count": len(chapters),
            "chapters":      chapters
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            "url":           url,
            "chapter_count": 0,
            "chapters":      []
        }

    finally:
        driver.quit()


# --- Run ---
url = "https://www.youtube.com/watch?v=K5KVEU3aaeQ&t=1s"
result = get_youtube_chapters(url)

if result:
    print(f"\n✅ Total chapters: {result['chapter_count']}")
    print("-" * 35)
    for i, ch in enumerate(result["chapters"], 1):
        print(f"{i}. [{ch['timestamp']}]  {ch['title']}")
import time
import tkinter as tk
from tkinter import ttk, scrolledtext
from selenium.webdriver.common.by import By

from utilitylib.driver import TableScraper

selectors = {
    "details_url": "https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/bond/BIP_CNTS03024V.xml&menuNo=416",
    "prc_url": "https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/bond/BIP_CNTS03025V.xml&menuNo=417",
    "corp_search_btn": "#bd_input2_image1",
    "corp_input": "#search_string",
    "corp_search": "#image2",
    "popup_frame": "#iframeIsin",
    "from_date_selector": "#inputCalendar1_input",
    "to_date_selector": "#inputCalendar2_input", 
}
kindDriver = TableScraper(headless=False)

def fmtkey(key):
    key=str(key).replace(' ','')
    types_str = [
        'EB', 'eb',
        'CB', 'cb',
        'BW', 'bw',
    ]
    for abbr in types_str: key=key.replace(abbr,'')
    idx=key.find('(')
    if idx!=-1: key=key[:idx]
    return key

def get_single_ticker(driver, corp_name, bond_name, from_date, to_date, buffer=0.3):
    print(f"Getting single ticker: {corp_name}")
    driver.setup()

    driver.open(selectors["details_url"])
    time.sleep(buffer)

    driver.click_button(selectors["corp_search_btn"])
    time.sleep(buffer)

    driver.fill_input(selectors["corp_input"], corp_name, selectors["popup_frame"])
    time.sleep(buffer)

    driver.click_button(selectors["corp_search"], selectors["popup_frame"])
    time.sleep(buffer)

    driver.switch_to_frame(selectors["popup_frame"])
    while True: # Keep searching until we find a match
        try:
            container = driver.driver.find_element(By.CSS_SELECTOR, "#isinList")
            items = container.find_elements(By.CSS_SELECTOR, '[id^="isinList_"][id$="_group178"]')
            searched_keys = []
            for _, item in enumerate(items):
                text = driver.driver.execute_script("return arguments[0].textContent.trim();", item) or ""
                searched_keys.append(fmtkey(text))
            pos = [i for i in range(len(searched_keys)) if searched_keys[i] == fmtkey(bond_name)]
            if pos:
                target = pos[0] # Use first match - error prevention
                print(f"Found match: position {target}")
                if len(pos) > 1:
                    print("Multiple matches found")
                break
            else: print("No matches found, retrying..."); time.sleep(buffer)          
        except Exception as e: print(f"Error finding container, retrying... {e}"); time.sleep(buffer)
    driver.click_button(f"#isinList_{target}_ISIN_ROW")
    driver.switch_to_default()
    time.sleep(buffer)

    driver.fill_input(selectors["from_date_selector"], from_date)
    time.sleep(buffer)

    driver.fill_input(selectors["to_date_selector"], to_date)
    time.sleep(buffer)

    driver.click_button(selectors["corp_search"])
    time.sleep(buffer)

    all_rows_dicts = []
    previous_page_key = None
    page_num = 1
    while True:
        try:
            # Parse current page table rows into dicts using a mapper
            def _row_mapper(values):
                row_dict = {}
                row_dict["title"] = corp_name
                # indices based on current table layout
                row_dict["date"] = values[5]
                row_dict["exc_amount"] = float(values[6].replace(',', '')) if values[6] else None
                row_dict["exc_shares"] = float(values[8].replace(',', '')) if values[8] else None
                row_dict["exc_price"] = float(values[9].replace(',', '')) if values[9] else None
                row_dict["listing_date"] = values[10]
                return row_dict

            data_dicts, rows = driver.table_to_dicts("#grid1_body_tbody", _row_mapper)

            page_key = driver.get_page_key(rows) if rows else None
            if previous_page_key is not None and page_key == previous_page_key: break # same page, stop

            all_rows_dicts.extend(data_dicts)
            previous_page_key = page_key
            # Check if current page is full (15 rows) - if not, no next page
            if len(rows) < 15: break
            try:
                driver.click_button("#gridPaging_next_btn")
                time.sleep(buffer)
                page_num += 1
            except Exception: break
        except Exception: break
    return all_rows_dicts

from export_results import read_list_titles, save_excel, clear_excel
class KINDScraperGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("SEIBRO Scraper")
        self.root.geometry("600x500")
        
        # Create GUI elements
        self.setup_gui()
        
        # Variables for tracking
        self.is_running = False
        self.scraper = None
        
    def setup_gui(self):
        title_label = tk.Label(self.root, text="SEIBRO Scraper", font=("Arial", 16, "bold"))
        title_label.pack(pady=10)
        
        status_frame = tk.Frame(self.root)
        status_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Label(status_frame, text="Status:", font=("Arial", 10, "bold")).pack(side="left")
        self.status_label = tk.Label(status_frame, text="Ready", fg="blue")
        self.status_label.pack(side="left", padx=5)
        
        progress_frame = tk.Frame(self.root)
        progress_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Label(progress_frame, text="Progress:", font=("Arial", 10, "bold")).pack(side="left")
        self.progress_var = tk.StringVar(value="0/0")
        self.progress_label = tk.Label(progress_frame, textvariable=self.progress_var)
        self.progress_label.pack(side="left", padx=5)
        
        self.progress_bar = ttk.Progressbar(self.root, mode='determinate')
        self.progress_bar.pack(fill="x", padx=10, pady=5)
        
        tk.Label(self.root, text="Log:", font=("Arial", 10, "bold")).pack(anchor="w", padx=10, pady=(10,0))
        self.log_text = scrolledtext.ScrolledText(self.root, height=15, width=70)
        self.log_text.pack(fill="both", expand=True, padx=10, pady=5)
        
        button_frame = tk.Frame(self.root)
        button_frame.pack(fill="x", padx=10, pady=10)
        
        self.start_button = tk.Button(button_frame, text="Start Scraping", command=self.start_scraping, 
                                    bg="green", fg="white", font=("Arial", 12, "bold"))
        self.start_button.pack(side="left", padx=5)
        
        self.stop_button = tk.Button(button_frame, text="Stop", command=self.stop_scraping, 
                                   bg="red", fg="white", font=("Arial", 12, "bold"), state="disabled")
        self.stop_button.pack(side="left", padx=5)
        
        self.clear_button = tk.Button(button_frame, text="Clear Log", command=self.clear_log)
        self.clear_button.pack(side="right", padx=5)
        
    def log(self, message):
        """Add message to log area"""
        self.log_text.insert(tk.END, f"{time.strftime('%H:%M:%S')} - {message}\n")
        self.log_text.see(tk.END)
        self.root.update_idletasks()
        
    def update_status(self, status, color="blue"):
        """Update status label"""
        self.status_label.config(text=status, fg=color)
        self.root.update_idletasks()
        
    def update_progress(self, current, total):
        """Update progress bar and label"""
        self.progress_var.set(f"{current}/{total}")
        if total > 0:
            self.progress_bar['value'] = (current / total) * 100
        self.root.update_idletasks()
        
    def clear_log(self):
        """Clear log area"""
        self.log_text.delete(1.0, tk.END)
        
    def start_scraping(self):
        """Start scraping"""
        if self.is_running:
            return
            
        self.is_running = True
        self.start_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.clear_log()
        
        # Start scraping directly
        self.run_scraping()

    def start(self, function): # Start 'function' function
        
        
        
    def stop_scraping(self):
        """Stop scraping"""
        self.is_running = False
        self.update_status("Stopping...", "orange")
        self.log("Stopping scraper...")
        
    def run_scraping(self):
        """Main scraping logic"""
        try:
            self.log("세이브로 데이터 다운로드를 시작합니다.")
            self.update_status("준비 중...", "blue")
            
            # Clear Excel sheets
            self.log("엑셀을 준비하는 중...")
            clear_excel(sheet_name="DB")
            clear_excel(sheet_name="EX")
            
            # Read company list
            self.log("회사 목록 읽는 중...")
            excel = read_list_titles()
            if not excel:
                self.log("엑셀 파일에 기업이 없습니다.")
                self.update_status("기업이 없습니다.", "red")
                return
                
            self.log(f"{len(excel)}개 기업을 발견했습니다.")
            
            # Create scraper
            base_config = {
                "from_date": "20210101",
                "to_date": time.strftime("%Y%m%d"),
                "headless": True,
                "display": False,
            }
            
            self.scraper = TableScraper(headless=True)
            self.scraper.setup()
            self.log("Chrome 브라우저가 정상적으로 실행되었습니다.")
            
            # Process details URL
            self.update_status("행사내역 데이터를 수집하는 중...", "blue")
            self.log("행사내역 데이터를 수집하는 중...\n")
            total_companies = len(excel)
            
            for i, item in enumerate(excel):
                if not self.is_running:
                    break
                    
                self.update_progress(i, total_companies)
                config = base_config.copy()
                config["company"] = item[1]
                config["keyword"] = item[0]
                is_first = (i == 0)
                
                self.log(f"{config['keyword']}의 행사내역 데이터를 수집하는 중... ({i+1}/{total_companies})")
                rows = get_single_ticker(self.scraper, config["company"], config["keyword"], config["from_date"], config["to_date"])
                
                if rows:
                    save_excel(rows, sheet_name="DB")
                    self.log(f"{config['keyword']}의 {len(rows)}개 데이터를 저장했습니다.\n")
                else:
                    self.log(f"{config['keyword']}의 해당하는 데이터가 없습니다.\n")
            
            if not self.is_running:
                return
            
            # Cleanup
            self.scraper.cleanup()
            self.log("Chrome 브라우저가 정상적으로 종료되었습니다.")
            
            self.update_progress(total_companies, total_companies)
            self.update_status("Completed!", "green")
            self.log("모든 데이터가 저장되었습니다.")
            self.log("데이터 수집이 완료되었습니다.")
            
        except Exception as e:
            self.log(f"Error: {str(e)}")
            self.update_status("오류가 발생했습니다.", "red")
        finally:
            self.is_running = False
            self.start_button.config(state="normal")
            self.stop_button.config(state="disabled")
            if self.scraper:
                try:
                    self.scraper.cleanup()
                except:
                    pass
    
    def run(self):
        self.root.mainloop()

if __name__ == "__main__":
    gui = KINDScraperGUI()
    gui.run()

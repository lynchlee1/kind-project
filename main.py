import time
import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import multiprocessing
from multiprocessing import Process, Manager
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

def get_single_ticker(driver, corp_name, bond_name, from_date, to_date, buffer=0.3, max_retries=3):
    """
    Scrape data for a single ticker using a shared driver.
    Note: driver must already be initialized (setup() called before).
    """
    print(f"Getting single ticker: {corp_name}")
    
    # Ensure driver is set up (should already be, but check to be safe)
    if not driver.driver:
        raise RuntimeError("Driver not initialized. Call driver.setup() first.")
    
    all_rows_dicts = []
    
    # Retry logic for handling errors
    for retry in range(max_retries):
        try:
            # Ensure we're in default content (not in a frame)
            try:
                driver.switch_to_default()
            except:
                pass
            
            # Check for and close any error popups before starting
            if driver.check_error_popup():
                print(f"Error popup detected, retrying ({retry + 1}/{max_retries})...")
                time.sleep(buffer * 2)  # Wait longer if error was detected
                if retry < max_retries - 1:
                    continue
                else:
                    raise Exception("Error popup appeared and could not be resolved")
            
            # Navigate to the details URL to start fresh
            driver.open(selectors["details_url"])
            time.sleep(buffer)
            
            # Check for error popup after page load
            if driver.check_error_popup():
                print(f"Error popup detected after page load, retrying ({retry + 1}/{max_retries})...")
                time.sleep(buffer * 2)
                if retry < max_retries - 1:
                    continue
                else:
                    raise Exception("Error popup appeared after page load")
            
            break  # Success, exit retry loop
        except Exception as e:
            if retry < max_retries - 1:
                print(f"Error in get_single_ticker (attempt {retry + 1}/{max_retries}): {e}")
                time.sleep(buffer * 2)
                continue
            else:
                raise
    
    try:

        driver.click_button(selectors["corp_search_btn"])
        time.sleep(buffer)
        
        # Check for error popup
        if driver.check_error_popup():
            raise Exception("Error popup appeared after clicking corp search button")

        driver.fill_input(selectors["corp_input"], corp_name, selectors["popup_frame"])
        time.sleep(buffer)
        
        # Check for error popup
        if driver.check_error_popup():
            raise Exception("Error popup appeared after filling corp input")

        driver.click_button(selectors["corp_search"], selectors["popup_frame"])
        time.sleep(buffer)
        
        # Check for error popup
        if driver.check_error_popup():
            raise Exception("Error popup appeared after clicking corp search in popup")

        driver.switch_to_frame(selectors["popup_frame"])
        max_search_retries = 10
        target = None
        for search_retry in range(max_search_retries):
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
                else: 
                    print(f"No matches found, retrying... ({search_retry + 1}/{max_search_retries})")
                    time.sleep(buffer)
            except Exception as e: 
                print(f"Error finding container, retrying... {e} ({search_retry + 1}/{max_search_retries})")
                time.sleep(buffer)
        
        if target is None:
            raise Exception(f"Could not find bond {bond_name} for company {corp_name} after {max_search_retries} retries")
        
        driver.click_button(f"#isinList_{target}_ISIN_ROW")
        driver.switch_to_default()
        time.sleep(buffer)
        
        # Check for error popup
        if driver.check_error_popup():
            raise Exception("Error popup appeared after selecting bond")

        driver.fill_input(selectors["from_date_selector"], from_date)
        time.sleep(buffer)

        driver.fill_input(selectors["to_date_selector"], to_date)
        time.sleep(buffer)

        driver.click_button(selectors["corp_search"])
        time.sleep(buffer)
        
        # Check for error popup before proceeding to table extraction
        if driver.check_error_popup():
            raise Exception("Error popup appeared after clicking search - request may have failed")

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
    finally:
        # Ensure we're back in default content after scraping (even if error occurred)
        try:
            driver.switch_to_default()
        except:
            pass
    
    return all_rows_dicts

from export_results import read_list_titles, save_excel, clear_excel

# Worker function for multiprocessing (must be at module level for pickling)
def process_company_worker(item, from_date, to_date, index, total_companies, excel_lock, result_queue):
    """Worker function for processing a single company in a separate process"""
    driver = None
    try:
        corp_name = item[1]
        bond_name = item[0]
        
        # Print to console (process-specific output)
        print(f"Processing {bond_name} ({index+1}/{total_companies})...")
        
        # Create a separate driver instance for this process with unique isolation
        driver = TableScraper(headless=False)
        driver.setup()  # Will use unique port and directories automatically
        
        # Scrape data using this process's own driver
        rows = get_single_ticker(
            driver, 
            corp_name, 
            bond_name, 
            from_date, 
            to_date
        )
        
        # Process-safe Excel writing
        if rows:
            try:
                with excel_lock:
                    save_excel(rows, sheet_name="DB")
                result = {
                    "success": True,
                    "keyword": bond_name,
                    "count": len(rows),
                    "index": index,
                    "message": f"{bond_name}의 {len(rows)}개 데이터를 저장했습니다."
                }
            except Exception as e:
                result = {
                    "success": False,
                    "keyword": bond_name,
                    "index": index,
                    "message": f"Error saving data for {bond_name}: {str(e)}"
                }
        else:
            result = {
                "success": True,
                "keyword": bond_name,
                "count": 0,
                "index": index,
                "message": f"{bond_name}의 해당하는 데이터가 없습니다."
            }
        
        # Send result back to main process via queue
        result_queue.put(result)
            
    except Exception as e:
        import traceback
        result = {
            "success": False,
            "keyword": item[0] if item else "unknown",
            "index": index,
            "message": f"Error processing {item[0] if item else 'unknown'}: {str(e)}\n{traceback.format_exc()}"
        }
        result_queue.put(result)
    finally:
        # Cleanup driver for this process
        if driver:
            try:
                driver.cleanup()
            except:
                pass

class KINDScraperGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("SEIBRO Scraper")
        self.root.geometry("600x500")
        
        # Create GUI elements
        self.setup_gui()
        
        # Variables for tracking
        self.is_running = False
        self.completed_count = 0  # Counter for completed tasks
        self.max_workers = 3  # Reduced to avoid overwhelming the website (was 5)
        self.processes = []  # List of Process objects
        self.manager = None  # Multiprocessing manager for shared locks
        self.excel_lock = None  # Process-safe lock for Excel operations
        self.result_queue = None  # Queue for receiving results from processes
        
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
        """Add message to log area (thread-safe)"""
        def _log():
            self.log_text.insert(tk.END, f"{time.strftime('%H:%M:%S')} - {message}\n")
            self.log_text.see(tk.END)
            self.root.update_idletasks()
        
        # Schedule GUI update in main thread
        self.root.after(0, _log)
        
    def update_status(self, status, color="blue"):
        """Update status label (thread-safe)"""
        def _update():
            self.status_label.config(text=status, fg=color)
            self.root.update_idletasks()
        
        # Schedule GUI update in main thread
        self.root.after(0, _update)
        
    def update_progress(self, current, total):
        """Update progress bar and label (thread-safe)"""
        def _update():
            self.progress_var.set(f"{current}/{total}")
            if total > 0:
                self.progress_bar['value'] = (current / total) * 100
            self.root.update_idletasks()
        
        # Schedule GUI update in main thread
        self.root.after(0, _update)
        
    def clear_log(self):
        """Clear log area"""
        self.log_text.delete(1.0, tk.END)
        
    def start_scraping(self):
        """Start scraping in a separate thread"""
        if self.is_running:
            return
            
        self.is_running = True
        self.start_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.clear_log()
        
        # Start scraping in a separate thread
        thread = threading.Thread(target=self.run_scraping)
        thread.daemon = True
        thread.start()
        
    def stop_scraping(self):
        """Stop scraping"""
        self.is_running = False
        self.update_status("Stopping...", "orange")
        self.log("Stopping scraper...")
        # Terminate all running processes
        for process in self.processes:
            try:
                process.terminate()
            except:
                pass
        # Wait for processes to finish
        for process in self.processes:
            try:
                process.join(timeout=1)
            except:
                pass
        self.processes = []
        
    def run_scraping(self):
        """Main scraping logic using multiprocessing.Process (like reference code)"""
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
            
            # Configuration
            from_date = "20210101"
            to_date = time.strftime("%Y%m%d")
            total_companies = len(excel)
            
            self.log(f"multiprocessing.Process를 사용하여 최대 {self.max_workers}개의 병렬 Chrome 드라이버로 처리합니다.")
            self.log("각 프로세스는 독립적인 Chrome 인스턴스를 사용합니다.")
            self.update_status("행사내역 데이터를 수집하는 중...", "blue")
            self.log("행사내역 데이터를 수집하는 중...\n")
            
            # Reset completed count
            self.completed_count = 0
            
            # Create multiprocessing manager for shared lock and queue
            self.manager = Manager()
            self.excel_lock = self.manager.Lock()
            self.result_queue = self.manager.Queue()
            
            # Start all processes (like reference code pattern)
            # Each process gets its own Chrome driver with unique port/directories
            self.processes = []
            for i, item in enumerate(excel):
                if not self.is_running:
                    break
                
                # Create and start process for each company
                process = Process(
                    target=process_company_worker,
                    args=(item, from_date, to_date, i, total_companies, self.excel_lock, self.result_queue)
                )
                process.start()
                self.processes.append(process)
                
                # Limit concurrent processes by waiting if we've reached max_workers
                while len([p for p in self.processes if p.is_alive()]) >= self.max_workers:
                    if not self.is_running:
                        break
                    # Check for results while waiting
                    self._check_results(total_companies)
                    time.sleep(0.5)  # Increased wait time
                
                # Longer delay between process starts to avoid rate limiting
                # This gives the website time to process each request
                time.sleep(1.0)  # Increased from 0.05 to 1.0 seconds
            
            self.log(f"총 {total_companies}개 기업을 {len(self.processes)}개의 프로세스로 병렬 처리 시작...\n")
            
            # Wait for all processes to finish (like reference code: join all)
            # Main waits for processes to finish
            for process in self.processes:
                if not self.is_running:
                    break
                # Check for results while waiting
                self._check_results(total_companies)
                try:
                    process.join()
                except:
                    pass
            
            # Collect any remaining results
            while not self.result_queue.empty():
                self._check_results(total_companies)
                time.sleep(0.1)
            
            if not self.is_running:
                self.log("스크래핑이 중단되었습니다.")
                return
            
            self.update_progress(total_companies, total_companies)
            self.update_status("Completed!", "green")
            self.log("모든 데이터가 저장되었습니다.")
            self.log("데이터 수집이 완료되었습니다.")
            
        except Exception as e:
            self.log(f"Error: {str(e)}")
            import traceback
            self.log(traceback.format_exc())
            self.update_status("오류가 발생했습니다.", "red")
        finally:
            self.is_running = False
            
            # Cleanup multiprocessing resources
            for process in self.processes:
                try:
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=1)
                except:
                    pass
            self.processes = []
            
            if self.manager:
                try:
                    self.manager.shutdown()
                except:
                    pass
                self.manager = None
            
            self.result_queue = None
            
            # Schedule GUI update in main thread
            self.root.after(0, lambda: self.start_button.config(state="normal"))
            self.root.after(0, lambda: self.stop_button.config(state="disabled"))
    
    def _check_results(self, total_companies):
        """Check result queue and update GUI"""
        try:
            while not self.result_queue.empty():
                result = self.result_queue.get_nowait()
                if result:
                    self.log(result.get("message", ""))
                    if result.get("success"):
                        self.completed_count += 1
                        self.update_progress(self.completed_count, total_companies)
        except:
            pass
    
    def run(self):
        self.root.mainloop()

if __name__ == "__main__":
    # Required for multiprocessing on Windows and some Unix systems
    multiprocessing.freeze_support()
    gui = KINDScraperGUI()
    gui.run()

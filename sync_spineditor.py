import os
import glob
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# ================= CẤU HÌNH API =================
# FILE JSON API CỦA SERVICE ACCOUNT
CREDENTIALS_FILE = 'gen-lang-client-0944295787-7c6a5ec1c046.json'
# ID CỦA MASTER SPREADSHEET (LẤY TỪ LINK CỦA BẠN)
SPREADSHEET_ID = '1lrm94N_ej-qbfGmRWVmx6WcmQHHXubCcH84t2UZJ8w8'
# THƯ MỤC CHỨA CÁC FILE CSV CỦA SPINEDITOR
DATA_FOLDER = 'data_spineditor'

# Áp dụng cho 3 Sheet
SHEET_NAMES = ['mohinhkientruc', 'architecturalmodel', 'mohinhsonganh']

def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    return client

def setup_sheets(client):
    try:
        sh = client.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        print(f"Error accessing Google Sheet {SPREADSHEET_ID}. Make sure it is shared to the service account.")
        print(f"Exception details: {e}")
        return None

    # Đảm bảo 3 Tabs đã tồn tại, nếu chưa có thì tạo mới
    existing_worksheets = [ws.title for ws in sh.worksheets()]
    for name in SHEET_NAMES:
        if name not in existing_worksheets:
            print(f"Creating new tab: {name}")
            sh.add_worksheet(title=name, rows="1000", cols="50")
            # Set up Headers
            ws = sh.worksheet(name)
            ws.update(range_name='A1:C1', values=[['Từ khóa', 'URL', 'Volume']])
            
    # Xoá Sheet mặc định "Trang tính1" nếu không cần
    try:
        sheet1 = sh.worksheet('Trang tính1')
        sh.del_worksheet(sheet1)
    except:
        pass

    return sh

def parse_date_from_filename(filename):
    # Ví dụ: 'mohinhkientruc 31032026.csv' -> '31032026'
    base = os.path.basename(filename).split('.')[0]
    parts = base.split(' ')
    if len(parts) >= 2:
        date_str = parts[-1]
        try:
            # Parse DDMMYYYY thành string chuẩn "Y-m-d" (optional) hoặc cứ giữ "DD/MM" cho ngắn gọn
            date_obj = datetime.strptime(date_str, "%d%m%Y")
            return date_obj.strftime("%d/%m")
        except ValueError:
            return date_str
    return "UnknownDate"

def get_website_from_filename(filename):
    base = os.path.basename(filename).split('.')[0]
    parts = base.split(' ')
    if len(parts) >= 2:
        web = parts[0]
        # Xoá các ký tự lạ
        return web
    return None

def sync_file_to_sheets(file_path, sh):
    website = get_website_from_filename(file_path)
    if website not in SHEET_NAMES:
        print(f"Bỏ qua file {file_path}. Tên website '{website}' không khớp với 3 dự án.")
        return

    date_label = parse_date_from_filename(file_path)
    print(f"Processing: {website} - Ngày {date_label}")
    
    # Read Data
    try:
        if file_path.lower().endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Lỗi đọc file {file_path}: {e}")
        return

    column_mapping = {
        'Từ khóa': 'keyword', 'Keyword': 'keyword',
        'Liên kết hiển thị': 'url', 'Link': 'url', 'URL': 'url',
        'Vị trí': 'rank', 'Thứ hạng': 'rank', 'Rank': 'rank',
        'Tìm kiếm/Tháng': 'vol', 'Volume': 'vol'
    }
    df = df.rename(columns=lambda x: column_mapping.get(x, x))
    if 'keyword' not in df.columns or 'rank' not in df.columns:
        print("Bỏ qua file do thiếu cột keyword hoặc rank.")
        return

    # Lấy Worksheet tương ứng
    ws = sh.worksheet(website)
    all_values = ws.get_all_values()
    if not all_values:
        ws.update('A1:C1', [['Từ khóa', 'URL', 'Volume']])
        all_values = [['Từ khóa', 'URL', 'Volume']]

    headers = all_values[0]
    
    # Kiểm tra Ngày này đã Sync chưa
    if date_label in headers:
        print(f"Cột ngày '{date_label}' đã tồn tại ở Google Sheets. Ghi đè cập nhật...")
        col_index = headers.index(date_label) + 1 # Gspread in dex 1-based
    else:
        print(f"Thêm cột ngày mới: '{date_label}'")
        col_index = len(headers) + 1
        ws.update_cell(1, col_index, date_label)
        headers.append(date_label)

    # Đưa all_values (List of Lists) ra xử lý mảng nội bộ cho nhanh
    # Tránh call API update từng ô siêu chậm
    
    key_row_map = {}
    for r_idx, row in enumerate(all_values):
        if r_idx == 0: continue
        kw = row[0].strip()
        key_row_map[kw] = r_idx + 1 # 1-based Google Sheets

    # Mảng để Update theo Batch
    new_rows_to_append = []
    updates = [] # List of {'range': 'D5', 'values': [[val]]}

    for _, row in df.iterrows():
        kw = str(row['keyword']).strip()
        
        # Xử lý rank > 100 -> trống
        try:
            rank_val = int(row['rank'])
            if rank_val > 100: rank_val = ""
        except:
            rank_val = ""

        # Check existing
        if kw in key_row_map:
            row_idx = key_row_map[kw]
            updates.append({
                'range': f'{gspread.utils.rowcol_to_a1(row_idx, col_index)}',
                'values': [[rank_val]]
            })
            # Cập nhật Volume nếu có thay đổi
            vol = str(row.get('vol', '')).replace(',', '')
            url = str(row.get('url', ''))
            updates.append({
                'range': f'B{row_idx}:C{row_idx}',
                'values': [[url, vol]]
            })
        else:
            # Thêm row mới
            # Format row array: [keyword, url, vol, "", "", ... , rank_val]
            new_row = [""] * col_index
            new_row[0] = kw
            new_row[1] = str(row.get('url', ''))
            new_row[2] = str(row.get('vol', '')).replace(',', '')
            new_row[-1] = rank_val
            new_rows_to_append.append(new_row)

    # Thực thi Batch Updates
    if updates:
        ws.batch_update(updates)
    
    if new_rows_to_append:
        ws.append_rows(new_rows_to_append)

    print(f"Xong file {file_path}! {len(df)} từ khóa đã được đồng bộ.")

def main():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        print(f"Đã tạo thư mục '{DATA_FOLDER}'. Hãy copy các file CSV/Excel vào đây và chạy lại!")
        return

    # Tìm các file csv, xls, xlsx
    csv_files = glob.glob(os.path.join(DATA_FOLDER, '*.csv'))
    excel_files = glob.glob(os.path.join(DATA_FOLDER, '*.xls*'))
    all_files = csv_files + excel_files
    
    if not all_files:
        print("Không tìm thấy file liệu nào trong thư mục data_spineditor.")
        return

    client = get_gspread_client()
    sh = setup_sheets(client)
    if not sh: return

    for f in all_files:
        sync_file_to_sheets(f, sh)

if __name__ == '__main__':
    main()

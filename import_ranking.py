import pandas as pd
import json
import os
from datetime import datetime

# Path to the historical data file
HISTORY_FILE = 'keyword_history.json'

def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"keywords": {}}

def save_history(data):
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def import_spin_editor_data(file_path):
    # Load history
    history = load_history()
    
    # Read the data file (Supports CSV or Excel)
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path)
    else:
        print("Unsupported file format. Please use CSV or Excel.")
        return

    # Standardizing column names (Mapping - Can be adjusted based on actual Spin Editor export)
    # Common Spin Editor columns: 'Từ khóa', 'Link', 'Thứ hạng', 'Tìm kiếm/Tháng'
    column_mapping = {
        'Từ khóa': 'keyword',
        'Keyword': 'keyword',
        'Liên kết hiển thị': 'url',
        'Link': 'url',
        'URL': 'url',
        'Vị trí': 'rank',
        'Thứ hạng': 'rank',
        'Rank': 'rank',
        'Tìm kiếm/Tháng': 'vol',
        'Volume': 'vol'
    }
    
    # Rename columns if they exist in mapping
    df = df.rename(columns=lambda x: column_mapping.get(x, x))
    
    # Required columns: keyword, rank
    if 'keyword' not in df.columns or 'rank' not in df.columns:
        print(f"Error: Required columns not found. Found: {df.columns.tolist()}")
        return

    today = datetime.now().strftime('%Y-%m-%d')
    
    for _, row in df.iterrows():
        kw = str(row['keyword'])
        rank = row['rank']
        url = row.get('url', '')
        vol = row.get('vol', 0)
        
        if kw not in history['keywords']:
            history['keywords'][kw] = {
                "url": url,
                "vol": int(vol) if pd.notnull(vol) else 0,
                "history": {}
            }
        
        # Always update URL and Vol if available
        if pd.notnull(url): history['keywords'][kw]['url'] = url
        if pd.notnull(vol): history['keywords'][kw]['vol'] = int(vol)
        
        # Add today's rank (handle non-numeric ranks like 'None' or '100+')
        try:
            rank_val = int(rank)
        except (ValueError, TypeError):
            rank_val = 101 # Default for out of top 100
            
        history['keywords'][kw]['history'][today] = rank_val

    save_history(history)
    update_html_dashboard(history)
    print(f"Successfully imported data for {len(df)} keywords on {today}.")

def update_html_dashboard(history):
    html_file = 'Keyword_Tracker_Dashboard.html'
    if not os.path.exists(html_file):
        return
        
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Find the keywordData variable and replace it
    start_marker = 'const keywordData = '
    end_marker = ';'
    
    start_idx = content.find(start_marker)
    if start_idx != -1:
        end_idx = content.find(end_marker, start_idx)
        if end_idx != -1:
            new_data_str = json.dumps(history, ensure_ascii=False, indent=4)
            new_content = content[:start_idx + len(start_marker)] + new_data_str + content[end_idx:]
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print("Updated embedded data in Keyword_Tracker_Dashboard.html")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        import_spin_editor_data(sys.argv[1])
    else:
        print("Usage: python import_ranking.py <path_to_file>")

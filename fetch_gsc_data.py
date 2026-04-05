import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
import datetime

# --- CONFIGURATION ---
# Path to your Google Service Account JSON key file
KEY_FILE = 'gen-lang-client-0944295787-7c6a5ec1c046.json'

# Sites to fetch (must be exact Property URLs in GSC)
SITES = [
    'https://mohinhkientruc.org/',
    'https://architecturalmodel.org/',
    'https://mohinhsonganh.com/'
]

# Days back to fetch (7 days for report + 7 days for comparison = 14 total)
DAYS_BACK = 14

def get_gsc_service(key_file):
    if not os.path.exists(key_file):
        raise FileNotFoundError(f"Key file not found: {key_file}. Please ensure the service account JSON is in the same directory.")
    
    scopes = ['https://www.googleapis.com/auth/webmasters.readonly']
    credentials = service_account.Credentials.from_service_account_file(key_file, scopes=scopes)
    return build('searchconsole', 'v1', credentials=credentials)

def fetch_data(service, site_url, start_date, end_date):
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['date'],
        'rowLimit': 1000
    }
    try:
        response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
        return response.get('rows', [])
    except Exception as e:
        print(f"Error fetching data for {site_url}: {e}")
        return None

def fetch_details(service, site_url, start_date, end_date):
    # Fetch Top Queries, Pages, Countries, and Devices
    results = {}
    dimensions = {
        'top_queries': ['query'],
        'top_pages': ['page'],
        'top_countries': ['country'],
        'search_types': ['searchAppearance'] # Just for structure
    }

    for key, dims in dimensions.items():
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': dims,
            'rowLimit': 10
        }
        try:
            response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
            results[key] = response.get('rows', [])
        except:
            results[key] = []
    
    # Specific Search Type fetch (Web, Image, Video)
    search_types = {}
    for s_type in ['web', 'image', 'video']:
        try:
            req = {'startDate': start_date, 'endDate': end_date, 'type': s_type}
            resp = service.searchanalytics().query(siteUrl=site_url, body=req).execute()
            rows = resp.get('rows', [])
            search_types[s_type] = sum(r['clicks'] for r in rows) if rows else 0
        except:
            search_types[s_type] = 0
    results['search_types'] = search_types

    return results

def calculate_trend(current, previous):
    if previous == 0:
        return '↑ 100%' if current > 0 else '--%'
    diff = ((current - previous) / previous) * 100
    arrow = '↑' if diff >= 0 else '↓'
    return f"{arrow} {abs(diff):.1f}%"

def calculate_trend_position(current, previous):
    # For position, DOWN is GOOD (1 is better than 10)
    if previous == 0: return '--%'
    diff = current - previous # Negative means improvement
    arrow = '↑' if diff <= 0 else '↓' # Arrow up means better rank (lower number)
    # But for consistency with other arrows (UP = better), we use Up for improvement
    return f"{arrow} {abs(diff):.1f}"

def run_sync():
    service = get_gsc_service(KEY_FILE)
    
    end_date = (datetime.date.today() - datetime.timedelta(days=3)).isoformat()
    start_date_7d = (datetime.date.today() - datetime.timedelta(days=9)).isoformat()
    start_date_14d = (datetime.date.today() - datetime.timedelta(days=16)).isoformat()

    all_data = {}

    for site in SITES:
        print(f"Processing {site}...")
        rows = fetch_data(service, site, start_date_14d, end_date)
        
        if not rows:
            print(f"No data for {site}. Skipping.")
            continue

        # Sort by date
        rows.sort(key=lambda x: x['keys'][0])
        
        # Split into current 7d and previous 7d
        # We need the last 14 entries or the entries within date ranges
        current_rows = [r for r in rows if r['keys'][0] >= start_date_7d]
        prev_rows = [r for r in rows if r['keys'][0] < start_date_7d]

        def get_totals(row_list):
            return {
                'clicks': sum(r['clicks'] for r in row_list),
                'impressions': sum(r['impressions'] for r in row_list),
                'ctr': sum(r['ctr'] for r in row_list) / len(row_list) if row_list else 0,
                'position': sum(r['position'] for r in row_list) / len(row_list) if row_list else 0
            }

        curr_totals = get_totals(current_rows)
        prev_totals = get_totals(prev_rows)

        details = fetch_details(service, site, start_date_7d, end_date)

        site_report = {
            'site_url': site,
            'period_label': f"{start_date_7d} to {end_date}",
            'metrics': {
                'clicks': {'total': curr_totals['clicks'], 'trend': calculate_trend(curr_totals['clicks'], prev_totals['clicks'])},
                'impressions': {'total': curr_totals['impressions'], 'trend': calculate_trend(curr_totals['impressions'], prev_totals['impressions'])},
                'ctr': {'total': f"{curr_totals['ctr']*100:.1f}%", 'trend': calculate_trend(curr_totals['ctr'], prev_totals['ctr'])},
                'position': {'total': f"{curr_totals['position']:.1f}", 'trend': calculate_trend_position(curr_totals['position'], prev_totals['position'])}
            },
            'daily_stats': [{'date': r['keys'][0], 'clicks': r['clicks'], 'impressions': r['impressions'], 'ctr': r['ctr'], 'position': r['position']} for r in current_rows],
            'top_queries': [{'query': r['keys'][0], 'clicks': r['clicks'], 'impressions': r['impressions']} for r in details['top_queries']],
            'top_pages': [{'page': r['keys'][0], 'clicks': r['clicks'], 'impressions': r['impressions']} for r in details['top_pages']],
            'top_countries': [{'country': r['keys'][0], 'clicks': r['clicks']} for r in details['top_countries']],
            'search_types': details['search_types']
        }
        
        all_data[site] = site_report

    with open('gsc_dashboard_data.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    print("GSC Data updated successfully.")

if __name__ == '__main__':
    run_sync()

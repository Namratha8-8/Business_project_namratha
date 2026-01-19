import requests
import pandas as pd
import time
from datetime import datetime

class UKInternetRetailScraperFast:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def scrape_posts(self, subreddit_name, total_posts=2000):
        """Scrape posts from subreddit - FAST"""
        print(f"\n🔍 Scraping r/{subreddit_name}...")
        
        all_posts = []
        after = None
        
        while len(all_posts) < total_posts:
            try:
                if after:
                    url = f'https://old.reddit.com/r/{subreddit_name}/hot/.json?limit=100&after={after}'
                else:
                    url = f'https://old.reddit.com/r/{subreddit_name}/hot/.json?limit=100'
                
                response = requests.get(url, headers=self.headers)
                
                if response.status_code == 429:
                    print(f"\n⏸️ Rate limited. Waiting 60s...")
                    time.sleep(60)
                    continue
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                posts = data['data']['children']
                after = data['data']['after']
                
                if not posts or after is None:
                    break
                
                all_posts.extend(posts)
                print(f"✓ {len(all_posts)}/{total_posts} posts...", end='\r')
                time.sleep(0.5)  # Minimal delay - 0.5 seconds
                
            except Exception as e:
                print(f"\n❌ Error: {e}")
                break
        
        print(f"\n✅ Got {len(all_posts)} posts")
        return all_posts
    
    def search_subreddit(self, subreddit_name, keywords, total_posts=1000):
        """Search for specific keywords - FAST"""
        print(f"\n🔎 Searching '{keywords}' in r/{subreddit_name}...")
        
        all_posts = []
        after = None
        iterations = 0
        max_iterations = 20
        
        while len(all_posts) < total_posts and iterations < max_iterations:
            try:
                if after:
                    url = f'https://old.reddit.com/r/{subreddit_name}/search.json?q={keywords}&restrict_sr=1&sort=relevance&limit=100&after={after}'
                else:
                    url = f'https://old.reddit.com/r/{subreddit_name}/search.json?q={keywords}&restrict_sr=1&sort=relevance&limit=100'
                
                response = requests.get(url, headers=self.headers)
                
                if response.status_code == 429:
                    print(f"\n⏸️ Rate limited. Waiting 60s...")
                    time.sleep(60)
                    continue
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                posts = data['data']['children']
                after = data['data']['after']
                
                if not posts or after is None:
                    break
                
                all_posts.extend(posts)
                print(f"✓ Found {len(all_posts)} posts...", end='\r')
                time.sleep(1)  # Reduced to 1 second for searches
                iterations += 1
                
            except Exception as e:
                print(f"\n❌ Error: {e}")
                break
        
        print(f"\n✅ Found {len(all_posts)} posts")
        return all_posts
    
    def scrape_comments(self, permalink):
        """Scrape comments from post - NO DELAY"""
        json_url = f"https://old.reddit.com{permalink}.json"
        
        try:
            response = requests.get(json_url, headers=self.headers)
            
            if response.status_code == 429:
                time.sleep(60)
                response = requests.get(json_url, headers=self.headers)
            
            if response.status_code != 200:
                return []
            
            data = response.json()
            comments = []
            
            if len(data) > 1:
                for comment in data[1]['data']['children']:
                    if comment['kind'] == 't1':
                        comment_data = comment['data']
                        author = comment_data.get('author', '[deleted]')
                        if author != 'AutoModerator':
                            comments.append({
                                'author': author,
                                'comment': comment_data.get('body', ''),
                                'score': comment_data.get('score', 0),
                                'created_utc': comment_data.get('created_utc', 0)
                            })
            
            return comments
        except:
            return []
    
    def convert_timestamp(self, timestamp):
        """Convert timestamp to date"""
        try:
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return ''
    
    def process_posts(self, posts, subreddit_name, category=''):
        """Process posts and extract comments - FAST"""
        if not posts:
            return []
        
        print(f"\n📝 Processing {len(posts)} posts...")
        print(f"⏱️ Est. time: {int(len(posts) * 0.5 / 60)} minutes (FAST MODE)\n")
        
        all_data = []
        rate_limit_count = 0
        
        for idx, post in enumerate(posts, 1):
            post_data = post['data']
            
            title = post_data.get('title', '')
            score = post_data.get('score', 0)
            created_utc = post_data.get('created_utc', 0)
            date_time = self.convert_timestamp(created_utc)
            permalink = post_data.get('permalink', '')
            selftext = post_data.get('selftext', '')
            
            print(f"[{idx}/{len(posts)}] {title[:50]}...", end='\r')
            
            comments = self.scrape_comments(permalink)
            
            if comments:
                for comment in comments:
                    all_data.append({
                        'subreddit': subreddit_name,
                        'category': category,
                        'post_title': title,
                        'post_score': score,
                        'post_datetime': date_time,
                        'post_text': selftext,
                        'comment_author': comment['author'],
                        'comment_text': comment['comment'],
                        'comment_score': comment['score'],
                        'comment_datetime': self.convert_timestamp(comment['created_utc'])
                    })
            else:
                all_data.append({
                    'subreddit': subreddit_name,
                    'category': category,
                    'post_title': title,
                    'post_score': score,
                    'post_datetime': date_time,
                    'post_text': selftext,
                    'comment_author': 'NO_COMMENTS',
                    'comment_text': '',
                    'comment_score': 0,
                    'comment_datetime': ''
                })
            
            # NO DELAY between posts! (removed time.sleep)
            # Only add small delay every 50 posts to avoid overwhelming
            if idx % 50 == 0:
                time.sleep(2)  # Small break every 50 posts
        
        print(f"\n✅ Collected {len(all_data)} rows")
        if rate_limit_count > 0:
            print(f"⚠️ Hit rate limit {rate_limit_count} times")
        return all_data
    
    def save_to_csv(self, data, filename):
        """Save to CSV"""
        if data:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"💾 Saved: {filename} ({len(df):,} rows)\n")
            return df
        return None


# ============================================
# MAIN SCRIPT - FAST VERSION
# ============================================

if __name__ == "__main__":
    scraper = UKInternetRetailScraperFast()
    
    print("="*70)
    print("    UK INTERNET RETAIL DATA SCRAPER - ⚡ FAST MODE")
    print("    Minimal delays for maximum speed!")
    print("="*70)
    
    all_data = []
    
    # ============================================
    # PART 1: Large UK Subreddits
    # ============================================
    print("\n" + "="*70)
    print("    PART 1: Large UK Subreddits")
    print("="*70)
    
    uk_subreddits = {
        'AskUK': 2000,
        'CasualUK': 2000,
        'UKPersonalFinance': 1500,
    }
    
    for subreddit, post_count in uk_subreddits.items():
        print(f"\n{'='*70}")
        print(f"    r/{subreddit}")
        print(f"{'='*70}")
        
        try:
            posts = scraper.scrape_posts(subreddit, post_count)
            if posts:
                data = scraper.process_posts(posts, subreddit, 'general_uk')
                all_data.extend(data)
                scraper.save_to_csv(data, f'{subreddit}_general.csv')
            time.sleep(5)  # Small delay between subreddits
        except Exception as e:
            print(f"❌ Error: {e}")
    
    # ============================================
    # PART 2: Online Shopping Keywords
    # ============================================
    print("\n" + "="*70)
    print("    PART 2: Online Shopping Keywords")
    print("="*70)
    
    online_shopping_searches = [
        # General online shopping
        ('AskUK', 'online shopping', 800, 'online_shopping'),
        ('AskUK', 'Amazon UK', 800, 'amazon'),
        ('AskUK', 'delivery', 800, 'delivery'),
        ('CasualUK', 'online order', 800, 'online_shopping'),
        ('CasualUK', 'parcel', 800, 'delivery'),
        
        # E-commerce platforms
        ('AskUK', 'ASOS', 600, 'fashion_ecommerce'),
        ('AskUK', 'eBay UK', 600, 'marketplace'),
        
        # Online grocery
        ('AskUK', 'Tesco delivery', 600, 'online_grocery'),
        ('AskUK', 'Ocado', 600, 'online_grocery'),
        ('CasualUK', 'supermarket delivery', 600, 'online_grocery'),
        
        # Food delivery
        ('AskUK', 'Deliveroo', 600, 'food_delivery'),
        ('AskUK', 'Just Eat', 600, 'food_delivery'),
        ('CasualUK', 'takeaway delivery', 600, 'food_delivery'),
        
        # Online retail experience
        ('UKPersonalFinance', 'online purchase', 600, 'online_shopping'),
        ('unitedkingdom', 'e-commerce', 600, 'ecommerce'),
    ]
    
    for subreddit, keyword, post_count, category in online_shopping_searches:
        print(f"\n{'='*70}")
        print(f"    r/{subreddit} - '{keyword}'")
        print(f"{'='*70}")
        
        try:
            posts = scraper.search_subreddit(subreddit, keyword, post_count)
            if posts:
                data = scraper.process_posts(posts, subreddit, category)
                all_data.extend(data)
                scraper.save_to_csv(data, f'{subreddit}_{keyword.replace(" ", "_")}.csv')
            time.sleep(5)  # Small delay between searches
        except Exception as e:
            print(f"❌ Error: {e}")
    
    # ============================================
    # SAVE COMBINED DATA
    # ============================================
    if all_data:
        print("\n" + "="*70)
        print("    SAVING COMBINED DATA")
        print("="*70)
        
        df_all = scraper.save_to_csv(all_data, 'UK_INTERNET_RETAIL_ALL.csv')
        
        print("="*70)
        print("    ✅ SCRAPING COMPLETE!")
        print("="*70)
        print(f"\n📊 Total rows collected: {len(all_data):,}")
        
        # Show category breakdown
        df_categories = pd.DataFrame(all_data)
        print(f"\n📈 Data by Category:")
        category_counts = df_categories['category'].value_counts()
        for cat, count in category_counts.items():
            print(f"   • {cat}: {count:,} rows")
        
        print(f"\n💡 Main file: UK_INTERNET_RETAIL_ALL.csv")
        print(f"\n⚡ Fast mode completed in ~{int(len(all_data) * 0.5 / 3600)} hours")

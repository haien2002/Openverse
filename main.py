# main.py

# arxiv最新文献的抓取与LLM拆解

# 核心参数
# DAYS_TO_FETCH = 几天内发表的文献
# PAPERS_PER_CATEGORY = 返回的文献数限制，None可以全出

# syl 20250627

import arxiv
import websocket
import datetime
import hashlib
import hmac
import base64
import json
import ssl
import sqlite3
import os
import time
import random
from urllib.parse import urlparse, urlencode
import threading

# ==============================================================================
# 1. 配置部分
# ==============================================================================
APPID = "548a4e52"
API_KEY = "abc0273b4e1c403f4849dc318b281464"
API_SECRET = "NGViNTE3N2FhYTVkOTQyZTQ0MzFhOGI4"
SPARK_URL = "wss://spark-api.xf-yun.com/v3.1/chat" 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "arxiv_papers.db")

# ==============================================================================
# 2. 数据库管理模块 (豪华版)
# ==============================================================================
def init_database():
    """初始化数据库，创建或更新论文表结构。"""
    print(f"🗄️ 正在初始化数据库 '{DB_FILE}'...")
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS papers (
                id TEXT PRIMARY KEY, category TEXT NOT NULL, title TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE, published TEXT NOT NULL, original_summary TEXT,
                spark_summary TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                authors TEXT, comment TEXT, journal_ref TEXT, doi TEXT
            )
        ''')
        existing_columns = [col[1] for col in cursor.execute("PRAGMA table_info(papers)").fetchall()]
        new_columns = { "authors": "TEXT", "comment": "TEXT", "journal_ref": "TEXT", "doi": "TEXT" }
        for col, col_type in new_columns.items():
            if col not in existing_columns:
                cursor.execute(f"ALTER TABLE papers ADD COLUMN {col} {col_type}")
        conn.commit()
    print("   - 数据库表结构准备就绪。")

def save_papers_to_db(papers_data):
    """将论文数据列表保存到 SQLite 数据库中，如果已存在则更新。"""
    if not papers_data: return
    print(f"💾 准备将 {len(papers_data)} 篇论文数据存入数据库...")
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        for paper in papers_data:
            paper_id = paper['url'].split('/abs/')[-1]
            cursor.execute('''
                INSERT OR REPLACE INTO papers (id, category, title, url, published, original_summary, 
                    spark_summary, authors, comment, journal_ref, doi)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                paper_id, paper.get('category', 'N/A'), paper.get('title', 'N/A'),
                paper.get('url', 'N/A'), paper.get('published', 'N/A'),
                paper.get('original_summary', ''), paper.get('spark_summary', ''),
                paper.get('authors', ''), paper.get('comment', ''),
                paper.get('journal_ref', ''), paper.get('doi', '')
            ))
        conn.commit()
    print(f"   - 数据保存/更新成功！")

# ==============================================================================
# 3. ArXiv 论文抓取模块 (反侦察版)
# ==============================================================================
def fetch_papers_in_range_robust(query, start_date, end_date):
    """在一个小时间段内，以非常稳健的方式抓取论文，包含智能重试和指数退避机制。"""
    precise_query = f"({query}) AND submittedDate:[{start_date.strftime('%Y%m%d%H%M%S')} TO {end_date.strftime('%Y%m%d%H%M%S')}]"
    print(f"   - 正在查询时间段: {start_date.date()} 到 {end_date.date()}")
    
    max_retries = 5
    base_delay = 10
    
    for attempt in range(max_retries):
        try:
            client = arxiv.Client(page_size=200, delay_seconds=5.0, num_retries=3)
            search = arxiv.Search(query=precise_query, sort_by=arxiv.SortCriterion.SubmittedDate)
            
            papers_in_chunk = []
            for result in client.results(search):
                author_names = ", ".join([author.name for author in result.authors])
                papers_in_chunk.append({
                    "title": result.title, "summary": result.summary.replace('\n', ' '),
                    "published": result.published.strftime("%Y-%m-%d"), "url": result.entry_id,
                    "authors": author_names, "comment": result.comment,
                    "journal_ref": result.journal_ref, "doi": result.doi
                })
            
            print(f"     ✅ 在此时间段内找到 {len(papers_in_chunk)} 篇论文。")
            return papers_in_chunk
        except Exception as e:
            print(f"     ❌ 第 {attempt + 1}/{max_retries} 次尝试失败: 侦测到服务器连接问题。")
            if attempt < max_retries - 1:
                sleep_time = base_delay * (2 ** attempt) + random.uniform(0, 5)
                print(f"     ...启动反侦察策略，将暂停 {sleep_time:.2f} 秒后重试...")
                time.sleep(sleep_time)
            else:
                print(f"     ❌ 所有重试均失败，跳过此时间段。错误: {e}")
                return []
    return []

# ==============================================================================
# 4. 讯飞星火 WebSocket 客户端 (保持不变)
# ==============================================================================
class SparkClient:
    def __init__(self):
        self.appid = APPID; self.api_key = API_KEY; self.api_secret = API_SECRET
        self.spark_url = SPARK_URL; self.domain = "generalv3"; self.result_text = ""
        self.is_ws_closed = threading.Event()
    def _generate_auth_url(self):
        host, path = urlparse(self.spark_url).netloc, urlparse(self.spark_url).path
        now = datetime.datetime.now(datetime.timezone.utc).strftime('%a, %d %b %Y %H:%M:%S GMT')
        signature_origin = f"host: {host}\ndate: {now}\nGET {path} HTTP/1.1"
        signature_sha = hmac.new(self.api_secret.encode('utf-8'), signature_origin.encode('utf-8'), digestmod=hashlib.sha256).digest()
        signature_sha_base64 = base64.b64encode(signature_sha).decode('utf-8')
        authorization_origin = f'api_key="{self.api_key}", algorithm="hmac-sha256", headers="host date request-line", signature="{signature_sha_base64}"'
        authorization = base64.b64encode(authorization_origin.encode('utf-8')).decode('utf-8')
        return self.spark_url + "?" + urlencode({ "authorization": authorization, "date": now, "host": host })
    def _on_message(self, ws, message):
        data = json.loads(message); code = data.get('header', {}).get('code', -1)
        if code != 0: print(f"Spark Error: {code}"); ws.close()
        content = data.get('payload', {}).get('choices', {}).get('text', [{}])[0].get('content', '')
        self.result_text += content
        if data.get('header', {}).get('status') == 2: ws.close()
    def _on_error(self, ws, error): print(f"Spark WebSocket Error: {error}"); self.is_ws_closed.set()
    def _on_close(self, ws, close_status_code, close_msg): self.is_ws_closed.set()
    def _on_open(self, ws, prompt):
        threading.Thread(target=lambda: ws.send(json.dumps({
            "header": {"app_id": self.appid, "uid": "harvester_user"},
            "parameter": {"chat": {"domain": self.domain, "temperature": 0.5, "max_tokens": 2048}},
            "payload": {"message": {"text": [{"role": "user", "content": prompt}]}}
        }))).start()
    def summarize_text(self, text_to_summarize):
        self.result_text = ""; self.is_ws_closed.clear()
        prompt = f"你是一位顶尖的科研助理，请使用中文，以专业、精炼、高度概括的语言风格，为以下科研论文摘要提炼出3个最重要的核心要点，并以 Markdown 的无序列表格式呈现。\n\n摘要内容：\n\"\"\"\n{text_to_summarize}\n\"\"\""
        ws = websocket.WebSocketApp(self._generate_auth_url(), on_message=self._on_message, on_error=self._on_error, on_close=self._on_close, on_open=lambda ws: self._on_open(ws, prompt))
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}); self.is_ws_closed.wait()
        return self.result_text.strip()

# ==============================================================================
# 5. 主程序入口
# ==============================================================================
def main_harvester():
    """运行工业级的数据采集和处理流程。"""
    if APPID == "你的_APPID": print("🛑 请先在脚本顶部填写你的讯飞星火 API 密钥信息！"); return
    
    init_database()
    
    # --- ✨ 在这里配置你的“指定航区”参数 ---
    # 例如：抓取 180 天前 到 540 天前的数据
    FETCH_END_DAYS_AGO = 180      # 航行结束点 (距离今天的天数)
    FETCH_START_DAYS_AGO = 540    # 航行起始点 (距离今天的天数)
    
    CHUNK_SIZE_DAYS = 30 # 每次API请求的时间窗口大小（天）
    
    # --- 计算航行参数 ---
    now = datetime.datetime.now(datetime.timezone.utc)
    window_end_date = now - datetime.timedelta(days=FETCH_END_DAYS_AGO)
    total_duration_days = FETCH_START_DAYS_AGO - FETCH_END_DAYS_AGO
    
    if total_duration_days <= 0:
        print("❌ 错误：抓取起始天数必须大于结束天数。")
        return

    print(f"🚢 “大航海”任务启动，目标航区：{FETCH_START_DAYS_AGO}天前 至 {FETCH_END_DAYS_AGO}天前。总航程：{total_duration_days}天。")

    TARGET_CATEGORIES = {"Biomolecules (生物分子)": "cat:q-bio.BM", "Cell Behavior (细胞行为)": "cat:q-bio.CB", "Genomics (基因组学)": "cat:q-bio.GN", "Molecular Networks (分子网络)": "cat:q-bio.MN", "Neurons and Cognition (神经元与认知)": "cat:q-bio.NC", "Other Quantitative Biology (其他定量生物学)": "cat:q-bio.OT", "Populations and Evolution (种群与进化)": "cat:q-bio.PE", "Quantitative Methods (定量方法)": "cat:q-bio.QM", "Subcellular Processes (亚细胞过程)": "cat:q-bio.SC", "Tissues and Organs (组织与器官)": "cat:q-bio.TO"}
    spark_summarizer = SparkClient()
    
    for category_name, category_query in TARGET_CATEGORIES.items():
        print(f"\n\n{'='*80}\n🧬 开始处理分类: {category_name}\n{'='*80}")
        
        all_papers_in_category = []
        
        # 按时间分片进行迭代抓取
        for i in range(0, total_duration_days, CHUNK_SIZE_DAYS):
            chunk_end_date = window_end_date - datetime.timedelta(days=i)
            chunk_start_date = window_end_date - datetime.timedelta(days=i + CHUNK_SIZE_DAYS)
            
            papers_chunk = fetch_papers_in_range_robust(category_query, chunk_start_date, chunk_end_date)
            all_papers_in_category.extend(papers_chunk)
            print(f"   --- 完成一个时间块的查询，礼貌性暂停3秒 ---")
            time.sleep(3)
        
        if not all_papers_in_category:
            print(f"  - 在分类 '{category_name}' 的指定总时间范围内未找到任何论文。")
            continue
            
        print(f"\n✨ 在分类 '{category_name}' 下共抓取到 {len(all_papers_in_category)} 篇论文，开始进行AI摘要...")
        
        processed_papers_for_db = []
        for i, paper in enumerate(all_papers_in_category):
            print(f"   --- 正在摘要第 {i+1}/{len(all_papers_in_category)} 篇: {paper['title'][:50]}...")
            spark_summary = spark_summarizer.summarize_text(paper['summary'])
            paper.update({"category": category_name, "spark_summary": spark_summary})
            processed_papers_for_db.append(paper)

        save_papers_to_db(processed_papers_for_db)

    print(f"\n\n{'='*80}\n🎉 恭喜！“指定航区”任务全部完成！所有数据均已存入数据库 '{DB_FILE}'\n{'='*80}")

if __name__ == '__main__':
    main_harvester()

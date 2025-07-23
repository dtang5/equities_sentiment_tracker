import praw
import pandas as pd
import re
from collections import defaultdict
from datetime import datetime
import yfinance as yf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


class RedditStockMentionCounter:
    def __init__(self, client_id, client_secret, user_agent):
        """
        Initialize Reddit API connection and sentiment analyzer

        Args:
            client_id: Reddit API client ID
            client_secret: Reddit API client secret
            user_agent: User agent string for Reddit API
        """
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )

        self.analyzer = SentimentIntensityAnalyzer()
        self.subreddits = ["wallstreetbets", "Shortsqueeze"]
        self.common_words = {
            "THE",
            "BE",
            "TO",
            "OF",
            "AND",
            "A",
            "IN",
            "THAT",
            "HAVE",
            "I",
            "IT",
            "FOR",
            "NOT",
            "ON",
            "WITH",
            "HE",
            "AS",
            "YOU",
            "DO",
            "AT",
            "THIS",
            "BUT",
            "HIS",
            "BY",
            "FROM",
            "UP",
            "ALL",
            "WOULD",
            "THERE",
            "THEIR",
            "WHAT",
            "SO",
            "OUT",
            "IF",
            "ABOUT",
            "WHO",
            "GET",
            "WHICH",
            "GO",
            "ME",
            "WHEN",
            "MAKE",
            "CAN",
            "LIKE",
            "TIME",
            "NO",
            "JUST",
            "HIM",
            "KNOW",
            "TAKE",
            "INTO",
            "YEAR",
            "YOUR",
            "GOOD",
            "SOME",
            "COULD",
            "THEM",
            "SEE",
            "OTHER",
            "THAN",
            "THEN",
            "NOW",
            "LOOK",
            "ONLY",
            "COME",
            "ITS",
            "OVER",
            "THINK",
            "ALSO",
            "BACK",
            "AFTER",
            "USE",
            "TWO",
            "HOW",
            "OUR",
            "WORK",
            "FIRST",
            "WELL",
            "WAY",
            "EVEN",
            "NEW",
            "WANT",
            "GIVE",
            "DAY",
            "MOST",
            "US",
            "ANY",
            "THESE",
            "SHE",
            "MAY",
            "SAY",
            "OR",
            "AN",
            "WILL",
            "MY",
            "ONE",
            "WAS",
            "IS",
            "ARE",
            "BEEN",
            "HAS",
            "HAD",
            "WERE",
            "SAID",
            "DID",
            "AM",
            "PM",
            "EST",
            "CST",
            "PST",
            "GMT",
            "USD",
            "EUR",
            "GBP",
            "DD",
            "YOLO",
            "LOL",
            "WTF",
            "IMO",
            "IMHO",
            "TBH",
            "IDK",
            "FYI",
            "EDIT",
            "UPDATE",
            "ETA",
            "CEO",
            "CFO",
            "ETF",
            "IPO",
            "ATH",
            "WSB",
            "FOMO",
            "HODL",
            "API",
            "USA",
            "LLC",
            "INC",
            "CORP",
            "FDA",
            "SEC",
            "NYSE",
            "NASDAQ",
        }

        self.valid_tickers = set()
        self.invalid_tickers = set()
        self.ticker_cache = {}

    def load_exchange_tickers(self):
        """Load tickers from multiple exchanges and sources"""
        print("Loading tickers from exchanges...")
        tickers = set()

        try:
            nasdaq_url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
            nasdaq_df = pd.read_csv(nasdaq_url, sep="|")
            nasdaq_df = nasdaq_df[nasdaq_df["Test Issue"] == "N"]
            nasdaq_symbols = nasdaq_df["Symbol"].dropna().tolist()
            tickers.update([s.strip() for s in nasdaq_symbols if s.strip() and len(s.strip()) <= 5])
            print(f"Loaded {len(nasdaq_symbols)} NASDAQ traded symbols")
        except Exception as e:
            print(f"Error loading NASDAQ tickers: {e}")

        try:
            nyse_url = "http://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
            nyse_df = pd.read_csv(nyse_url, sep="|")
            nyse_df = nyse_df[nyse_df["Test Issue"] == "N"]
            nyse_symbols = nyse_df["ACT Symbol"].dropna().tolist()
            tickers.update([s.strip() for s in nyse_symbols if s.strip() and len(s.strip()) <= 5])
            print(f"Loaded {len(nyse_symbols)} NYSE/AMEX symbols")
        except Exception as e:
            print(f"Error loading NYSE tickers: {e}")

        etfs = {
            "SPY",
            "QQQ",
            "IWM",
            "DIA",
            "VOO",
            "VTI",
            "GLD",
            "SLV",
            "USO",
            "TLT",
            "IEF",
            "LQD",
            "HYG",
            "EEM",
            "EFA",
            "VNQ",
            "ARKK",
            "ARKG",
            "ARKF",
            "ARKQ",
            "ARKW",
            "XLE",
            "XLF",
            "XLK",
            "XLV",
            "XLI",
            "XLY",
            "XLP",
            "XLB",
            "XLRE",
            "XLU",
            "VXX",
            "UVXY",
            "SQQQ",
            "TQQQ",
            "SPXU",
            "SPXS",
            "SH",
            "PSQ",
            "QID",
            "SOXL",
            "SOXS",
            "JETS",
            "ICLN",
            "TAN",
        }
        tickers.update(etfs)

        popular_stocks = {
            "GME",
            "AMC",
            "BB",
            "NOK",
            "PLTR",
            "TSLA",
            "AAPL",
            "MSFT",
            "AMZN",
            "BABA",
            "TSM",
            "NIO",
            "XPEV",
            "LI",
            "BIDU",
            "JD",
            "PDD",
            "NTES",
            "SNDL",
            "TLRY",
            "ACB",
            "HEXO",
            "OGI",
            "CRON",
            "CGC",
            "APHA",
            "NAKD",
            "CTRM",
            "SHIP",
            "TOPS",
            "ZOM",
            "OCGN",
            "BNGO",
            "SENS",
            "CLOV",
            "WKHS",
            "RIDE",
            "NKLA",
            "HYLN",
            "GOEV",
            "FSR",
            "WISH",
            "SOFI",
            "HOOD",
            "COIN",
            "RBLX",
            "ABNB",
            "DASH",
            "UBER",
            "LYFT",
        }
        tickers.update(popular_stocks)

        # Filter out common words
        self.valid_tickers = {t for t in tickers if t not in self.common_words}
        print(f"Total tickers loaded: {len(self.valid_tickers)}")

    def validate_ticker_yahoo(self, ticker):
        """
        Validate a ticker using Yahoo Finance API

        Args:
            ticker: Stock ticker to validate

        Returns:
            True if valid ticker, False otherwise
        """
        # Check cache first
        if ticker in self.ticker_cache:
            return self.ticker_cache[ticker]

        try:
            stock = yf.Ticker(ticker)
            # Try to get basic info
            info = stock.info

            # Check if we got actual data back
            is_valid = bool(
                info
                and (
                    info.get("symbol")
                    or info.get("regularMarketPrice")
                    or info.get("previousClose")
                    or info.get("marketCap")
                    or info.get("longName")
                    or info.get("shortName")
                )
            )

            # Cache the result
            self.ticker_cache[ticker] = is_valid
            return is_valid

        except:
            self.ticker_cache[ticker] = False
            return False

    def validate_tickers_batch(self, tickers):
        """
        Validate multiple tickers in parallel

        Args:
            tickers: List of tickers to validate

        Returns:
            Set of valid tickers
        """
        valid = set()
        to_check = []

        for ticker in tickers:
            if ticker in self.valid_tickers:
                valid.add(ticker)
            elif ticker in self.invalid_tickers:
                continue
            else:
                to_check.append(ticker)

        if to_check and len(to_check) <= 20:
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_ticker = {executor.submit(self.validate_ticker_yahoo, ticker): ticker for ticker in to_check}

                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        if future.result():
                            valid.add(ticker)
                            self.valid_tickers.add(ticker)
                        else:
                            self.invalid_tickers.add(ticker)
                    except Exception as e:
                        print(f"Error validating ticker {ticker}: {e}")
                        self.invalid_tickers.add(ticker)

        return valid

    def extract_tickers(self, text):
        """
        Extract potential stock tickers from text with improved validation

        Args:
            text: Text to extract tickers from

        Returns:
            List of validated tickers found in text
        """
        if not text:
            return []

        tickers_found = []

        # Pattern 1: $TICKER format (high confidence)
        dollar_tickers = re.findall(r"\$([A-Z]{1,5})\b", text)
        tickers_found.extend(dollar_tickers)

        # Pattern 2: Tickers in context (e.g., "Buy AAPL", "TSLA calls")
        context_patterns = [
            r"(?:buy|sell|long|short|calls?|puts?|hold|bought|sold)\s+([A-Z]{2,5})\b",
            r"([A-Z]{2,5})\s+(?:calls?|puts?|shares?|stock|options?|\d+[cp])",
            r"\b([A-Z]{2,5})\s+(?:to the moon|mooning|squeeze|gamma)",
            r"\b([A-Z]{2,5})\s+\$?\d+",  # AAPL 150 or AAPL $150
        ]

        for pattern in context_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            tickers_found.extend([m.upper() for m in matches if m.upper()])

        # Pattern 3: Standalone tickers (lower confidence)
        # Only if surrounded by clear boundaries
        standalone = re.findall(r"(?:^|\s|[.,!?;:(])([A-Z]{2,5})(?:$|\s|[.,!?;:)])", text)

        potential_tickers = set(tickers_found + standalone)

        filtered_tickers = {t for t in potential_tickers if t not in self.common_words and 2 <= len(t) <= 5}

        valid_tickers = self.validate_tickers_batch(filtered_tickers)

        return list(valid_tickers)

    def analyze_sentiment(self, text):
        """Analyze sentiment of text using VADER"""
        scores = self.analyzer.polarity_scores(text)
        return scores

    def scan_subreddit(self, subreddit_name, time_filter="hot", limit=100):
        """Scan a subreddit for stock mentions"""
        print(f"\nScanning r/{subreddit_name} ({time_filter})...")

        ticker_data = defaultdict(
            lambda: {
                "count": 0,
                "sentiment": {"positive": 0, "negative": 0, "neutral": 0, "compound": 0},
                "posts": [],
                "dollar_mentions": 0,
                "context_mentions": 0,
            }
        )

        try:
            subreddit = self.reddit.subreddit(subreddit_name)

            if time_filter == "hot":
                posts = subreddit.hot(limit=limit)
            elif time_filter == "new":
                posts = subreddit.new(limit=limit)
            elif time_filter == "top":
                posts = subreddit.top(time_filter="day", limit=limit)
            else:
                posts = subreddit.hot(limit=limit)

            post_count = 0
            for post in posts:
                post_count += 1

                if not post.title or post.selftext == "[removed]":
                    continue
                full_text = f"{post.title} {post.selftext}"

                dollar_mentions = set(re.findall(r"\$([A-Z]{1,5})\b", full_text))
                tickers = self.extract_tickers(full_text)
                sentiment = self.analyze_sentiment(full_text)

                # Update ticker data
                for ticker in set(tickers):
                    ticker_data[ticker]["count"] += 1
                    ticker_data[ticker]["sentiment"]["compound"] += sentiment["compound"]

                    if ticker in dollar_mentions:
                        ticker_data[ticker]["dollar_mentions"] += 1

                    context_pattern = f"(?:buy|sell|long|short|calls?|puts?)\s+{ticker}"
                    if re.search(context_pattern, full_text, re.IGNORECASE):
                        ticker_data[ticker]["context_mentions"] += 1

                    if sentiment["compound"] >= 0.05:
                        ticker_data[ticker]["sentiment"]["positive"] += 1
                    elif sentiment["compound"] <= -0.05:
                        ticker_data[ticker]["sentiment"]["negative"] += 1
                    else:
                        ticker_data[ticker]["sentiment"]["neutral"] += 1
                    ticker_data[ticker]["posts"].append(
                        {
                            "title": post.title[:100],
                            "score": post.score,
                            "num_comments": post.num_comments,
                            "created_utc": post.created_utc,
                            "url": f"https://reddit.com{post.permalink}",
                            "sentiment": sentiment["compound"],
                        }
                    )
                time.sleep(0.1)

            print(f"  Scanned {post_count} posts, found {len(ticker_data)} unique tickers")

        except Exception as e:
            print(f"Error scanning r/{subreddit_name}: {e}")

        return ticker_data

    def scan_all_subreddits(self, time_filter="hot", limit=50):
        """Scan all configured subreddits for stock mentions"""
        if not self.valid_tickers:
            self.load_exchange_tickers()

        all_ticker_data = defaultdict(
            lambda: {
                "count": 0,
                "sentiment": {"positive": 0, "negative": 0, "neutral": 0, "compound": 0},
                "posts": [],
                "subreddits": set(),
                "dollar_mentions": 0,
                "context_mentions": 0,
            }
        )

        for subreddit in self.subreddits:
            subreddit_data = self.scan_subreddit(subreddit, time_filter, limit)

            for ticker, data in subreddit_data.items():
                all_ticker_data[ticker]["count"] += data["count"]
                all_ticker_data[ticker]["sentiment"]["positive"] += data["sentiment"]["positive"]
                all_ticker_data[ticker]["sentiment"]["negative"] += data["sentiment"]["negative"]
                all_ticker_data[ticker]["sentiment"]["neutral"] += data["sentiment"]["neutral"]
                all_ticker_data[ticker]["sentiment"]["compound"] += data["sentiment"]["compound"]
                all_ticker_data[ticker]["posts"].extend(data["posts"])
                all_ticker_data[ticker]["subreddits"].add(subreddit)
                all_ticker_data[ticker]["dollar_mentions"] += data["dollar_mentions"]
                all_ticker_data[ticker]["context_mentions"] += data["context_mentions"]

        return all_ticker_data

    def get_stock_info(self, ticker):
        """Get current stock information using yfinance"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            current_price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            previous_close = info.get("previousClose") or info.get("regularMarketPreviousClose") or 0

            if current_price and previous_close:
                change_percent = ((current_price - previous_close) / previous_close) * 100
            else:
                change_percent = 0

            return {
                "ticker": ticker,
                "company_name": info.get("longName") or info.get("shortName") or ticker,
                "current_price": current_price,
                "previous_close": previous_close,
                "change_percent": change_percent,
                "volume": info.get("volume", 0),
                "market_cap": info.get("marketCap", 0),
                "sector": info.get("sector", "N/A"),
                "exchange": info.get("exchange", "N/A"),
            }
        except Exception as e:
            print(f"Error fetching stock info for {ticker}: {e}")
            return {
                "ticker": ticker,
                "company_name": ticker,
                "current_price": 0,
                "previous_close": 0,
                "change_percent": 0,
                "volume": 0,
                "market_cap": 0,
                "sector": "N/A",
                "exchange": "N/A",
            }

    def calculate_confidence_score(self, data):
        """Calculate confidence score for ticker mention"""
        score = 0

        if data["dollar_mentions"] > 0:
            score += min(data["dollar_mentions"] * 20, 40)

        if data["context_mentions"] > 0:
            score += min(data["context_mentions"] * 15, 30)

        if len(data["subreddits"]) > 1:
            score += len(data["subreddits"]) * 5

        if data["count"] > 10:
            score += 10
        elif data["count"] > 5:
            score += 5

        return min(score, 100)

    def generate_report(self, ticker_data, top_n=50):
        """Generate a report of top mentioned stocks"""
        report_data = []

        for ticker in ticker_data:
            ticker_data[ticker]["confidence"] = self.calculate_confidence_score(ticker_data[ticker])

        sorted_tickers = sorted(
            ticker_data.items(), key=lambda x: x[1]["count"] * (x[1]["confidence"] / 100), reverse=True
        )[:top_n]

        print(f"\nGenerating report for top {top_n} mentioned stocks...")

        for ticker, data in sorted_tickers:
            stock_info = self.get_stock_info(ticker)

            total_sentiment_posts = sum(
                [data["sentiment"]["positive"], data["sentiment"]["negative"], data["sentiment"]["neutral"]]
            )

            if total_sentiment_posts > 0:
                avg_sentiment = data["sentiment"]["compound"] / total_sentiment_posts
                sentiment_ratio = data["sentiment"]["positive"] / total_sentiment_posts * 100
            else:
                avg_sentiment = 0
                sentiment_ratio = 0

            dollar_ratio = (data["dollar_mentions"] / data["count"] * 100) if data["count"] > 0 else 0
            context_ratio = (data["context_mentions"] / data["count"] * 100) if data["count"] > 0 else 0

            report_data.append(
                {
                    "Ticker": ticker,
                    "Company": stock_info["company_name"][:30],
                    "Mentions": data["count"],
                    "Confidence": f"{data['confidence']}%",
                    "$ Ratio": f"{dollar_ratio:.0f}%",
                    "Context Ratio": f"{context_ratio:.0f}%",
                    "Positive %": f"{sentiment_ratio:.1f}%",
                    "Price": f"${stock_info['current_price']:.2f}" if stock_info["current_price"] else "N/A",
                    "Change": f"{stock_info['change_percent']:+.2f}%" if stock_info["change_percent"] else "N/A",
                    "Market Cap": f"${stock_info['market_cap'] / 1e9:.1f}B"
                    if stock_info["market_cap"] > 1e9
                    else "N/A",
                    "Subreddits": len(data["subreddits"]),
                }
            )

            time.sleep(0.2)

        return pd.DataFrame(report_data)

    def save_results(self, ticker_data, file_name_suffix=""):
        """Save results to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reddit_stock_mentions_{file_name_suffix}_{timestamp}"

        df = self.generate_report(ticker_data)

        csv_filename = f"{filename}.csv"
        df.to_csv(csv_filename, index=False)
        print(f"\nResults saved to {csv_filename}")

        print("\nTop 20 Most Mentioned Stocks (by confidence-weighted score):")
        print(df.head(20).to_string(index=False))

        print("\n" + "=" * 80)
        print("KEY INSIGHTS")
        print("=" * 80)

        high_confidence = df[df["Confidence"].str.rstrip("%").astype(int) >= 70]
        if not high_confidence.empty:
            print("\nHigh Confidence Mentions (70%+ confidence):")
            for _, row in high_confidence.head(10).iterrows():
                print(f"  {row['Ticker']}: {row['Mentions']} mentions, {row['Confidence']} confidence")

        return df


def main():
    CLIENT_ID = "PtTjBJMYMQOphot7TtV7rg"
    CLIENT_SECRET = "vjvdPUe1-4kyscspqpbfjHVshZPfng"
    USER_AGENT = "StockMentionCounter/1.0 by Daniel Tang"

    counter = RedditStockMentionCounter(CLIENT_ID, CLIENT_SECRET, USER_AGENT)

    for time_filter in ["new", "hot"]:
        ticker_data = counter.scan_all_subreddits(time_filter=time_filter, limit=200)

        df = counter.save_results(ticker_data, file_name_suffix=time_filter)

        print("\n" + "=" * 80)
        print(f"SUMMARY STATISTICS {time_filter.upper()}")
        print("=" * 80)

        total_mentions = sum(data["count"] for data in ticker_data.values())
        print(f"Total stock mentions found: {total_mentions}")
        print(f"Unique stocks mentioned: {len(ticker_data)}")
        print(f"Average mentions per stock: {total_mentions / len(ticker_data):.1f}")


if __name__ == "__main__":
    main()

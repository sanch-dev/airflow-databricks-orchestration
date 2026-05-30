# ============================================================================
# KAFKA PRODUCER: NewsAPI → Kafka
# ============================================================================
# Purpose: Fetch real news articles from NewsAPI and send them to Kafka
# Flow: NewsAPI (source) → Python script → Kafka topic → Databricks (consumer)
# 
# This script demonstrates:
# 1. Fetching data from an external API (NewsAPI)
# 2. Error handling (try-except blocks)
# 3. Sending data to Kafka message broker
# 4. Monitoring what gets sent
# ============================================================================

import os

import requests
# Purpose: Make HTTP requests to external APIs (NewsAPI)
# We use this to fetch news articles from the web

import json
# Purpose: Convert Python objects to JSON format (and vice versa)
# We need JSON because Kafka expects messages in JSON format

from kafka import KafkaProducer
# Purpose: Send messages to Kafka
# This is the main tool for producing messages

import time
# Purpose: Time-related operations (delays, timestamps, etc.)
# We import it in case we need to add delays between sends

from datetime import datetime
# Purpose: Get current date and time
# We use this to timestamp when each message was produced


# ============================================================================
# CONFIGURATION (Settings for our script)
# ============================================================================

# NewsAPI Configuration
NEWS_API_KEY = os.getenv('NEWS_API_KEY', 'YOUR_NEWS_API_KEY')
# → This proves you're authorized to use NewsAPI

NEWS_API_URL = "https://newsapi.org/v2/top-headlines"
# → This is the NewsAPI endpoint we're calling
# → Specifically the "top headlines" endpoint (not search, not everything)
# → We could use other endpoints like /everything or /sources

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ["20.193.141.136:9092"]
# → Where Kafka is running (your Azure VM)
# → Format: [IP:PORT]
# → Could be multiple servers: ["server1:9092", "server2:9092", "server3:9092"]

KAFKA_TOPIC = "news"
# → The Kafka topic name we created earlier
# → Messages will be sent TO this topic
# → Other systems (like Databricks) will read FROM this topic


# ============================================================================
# FUNCTION 1: FETCH NEWS FROM API
# ============================================================================

def get_news_from_api():
    """
    Purpose: Fetch articles from NewsAPI
    
    What it does:
    1. Prepares parameters for the NewsAPI request
    2. Makes an HTTP GET request to NewsAPI
    3. Extracts the articles from the response
    4. Returns the list of articles
    
    Returns:
    - List of article dictionaries (if successful)
    - Empty list [] (if error)
    """
    
    # Create parameters for the API request
    params = {
        'country': 'us',           # Get news from USA only
        'apiKey': NEWS_API_KEY,    # Send authentication (your API key)
        'pageSize': 10,            # Get 10 articles (not 100, not 1)
        'sortBy': 'publishedAt'    # Sort by newest first
    }
    
    # Try to fetch from NewsAPI (with error handling)
    try:
        # Make the HTTP GET request to NewsAPI
        response = requests.get(NEWS_API_URL, params=params)
        
        # Check if the request was successful
        # If NewsAPI returned an error (401, 403, 500, etc.), raise an exception
        response.raise_for_status()
        
        # Convert the response from JSON to Python dictionary
        # Then extract the 'articles' field (if it doesn't exist, use empty list)
        articles = response.json().get('articles', [])
        
        # Print success message with the number of articles fetched
        print(f"✓ Fetched {len(articles)} articles from NewsAPI")
        
        # Return the articles to whoever called this function
        return articles
    
    # If ANY error happens, catch it
    except Exception as e:
        # Print the error message
        print(f"✗ Error fetching from NewsAPI: {e}")
        
        # Return empty list (no articles to send)
        return []


# ============================================================================
# FUNCTION 2: SEND ARTICLES TO KAFKA
# ============================================================================

def send_to_kafka(articles):
    """
    Purpose: Send articles to Kafka message broker
    
    What it does:
    1. Create a connection to Kafka
    2. Loop through each article
    3. Convert each article to a message
    4. Send each message to Kafka
    5. Track how many were sent successfully
    
    Args:
    - articles: List of article dictionaries from NewsAPI
    
    Returns:
    - Number of articles successfully sent to Kafka
    """
    
    # Try to send articles to Kafka (with error handling)
    try:
        # Create a KafkaProducer object (this handles sending messages)
        producer = KafkaProducer(
            # Where Kafka is running
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            
            # How to convert messages before sending
            # lambda = anonymous function that takes value 'v'
            # json.dumps(v) = convert dict to JSON string
            # .encode('utf-8') = convert string to bytes (Kafka needs bytes)
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            
            # Wait for all brokers to confirm before moving on (safety first)
            acks='all',
            
            # If sending fails, try 3 more times before giving up
            retries=3
        )
        
        # Initialize counter to track successful sends
        sent_count = 0
        
        # Loop through each article
        # enumerate gives us: i=index (0,1,2...) and article=the actual article
        for i, article in enumerate(articles):
            
            # Create a clean, structured message from the article
            # We're extracting specific fields and adding our own
            message = {
                'id': i,                                              # Message number (0, 1, 2, ...)
                'title': article.get('title', ''),                  # Article title (use '' if missing)
                'description': article.get('description', ''),      # Short description
                'content': article.get('content', ''),              # Full article content
                'url': article.get('url', ''),                      # Link to article
                'source': article.get('source', {}).get('name', 'Unknown'),  # Source name (nested get)
                'author': article.get('author', 'Unknown'),         # Who wrote it
                'publishedAt': article.get('publishedAt', ''),      # When it was published
                'imageUrl': article.get('urlToImage', ''),          # Picture URL
                'produced_at': datetime.now().isoformat()           # When we produced this message (now)
            }
            
            # Try to send this article to Kafka
            try:
                # Send the message to Kafka
                # Returns a "future" object (async, not immediate)
                future = producer.send(KAFKA_TOPIC, value=message)
                
                # Wait up to 10 seconds for confirmation that Kafka received it
                record_metadata = future.get(timeout=10)
                
                # If we got here, it was successful!
                # Print success message with article title (first 60 chars only)
                print(f"✓ Sent [{i+1}]: {article.get('title', 'Unknown')[:60]}")
                
                # Increment the counter (we sent 1 more article)
                sent_count += 1
            
            # If this specific article failed to send
            except Exception as e:
                # Print error but don't stop the whole process
                print(f"✗ Failed to send article {i+1}: {e}")
        
        # After trying to send all articles, flush any remaining messages
        # This ensures ALL messages are sent before closing
        producer.flush()
        
        # Close the Kafka connection (cleanup)
        producer.close()
        
        # Print summary of what was sent
        print(f"\n✓ Successfully sent {sent_count}/{len(articles)} articles to Kafka topic '{KAFKA_TOPIC}'")
        
        # Return how many we successfully sent
        return sent_count
    
    # If ANYTHING goes wrong with Kafka itself
    except Exception as e:
        # Print error
        print(f"✗ Error with Kafka producer: {e}")
        
        # Return 0 (sent nothing)
        return 0


# ============================================================================
# FUNCTION 3: MAIN ORCHESTRATION
# ============================================================================

def main():
    """
    Purpose: Orchestrate the entire pipeline
    
    Flow:
    1. Print a nice header
    2. Fetch articles from NewsAPI
    3. If we got articles, send them to Kafka
    4. Print final result
    """
    
    # Print a nice header to show the script is starting
    print("=" * 60)
    print("Kafka Producer: NewsAPI → Kafka")
    print("=" * 60)
    
    # Step 1: Fetch articles from NewsAPI
    # This calls the get_news_from_api() function
    articles = get_news_from_api()
    
    # Step 2: Check if we got any articles
    # If not, stop here (no point sending nothing)
    if not articles:
        print("✗ No articles to send")
        return  # Exit the function
    
    # Step 3: Send articles to Kafka
    # This calls the send_to_kafka() function
    sent = send_to_kafka(articles)
    
    # Step 4: Print final result
    # Check if we sent at least 1 article
    if sent > 0:
        print("\n✓ Producer complete!")
    else:
        print("\n✗ Producer failed")


# ============================================================================
# ENTRY POINT (When the script runs)
# ============================================================================

if __name__ == "__main__":
    """
    Purpose: Run main() ONLY if this file is executed directly
    
    Explanation:
    - __name__ is a special Python variable
    - If you run: python kafka_producer.py → __name__ = "__main__"
    - If you import: from kafka_producer import X → __name__ = "kafka_producer"
    
    Why we use this:
    - When someone imports this file, we don't want main() to run automatically
    - We want to give them control over what happens
    - This pattern allows both direct execution AND function importing
    """
    
    # Run the main function
    main()
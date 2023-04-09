import snscrape.modules.twitter as sntwitter
import pandas as pd
import streamlit as st
import datetime
from pymongo import MongoClient

tweets_df = pd.DataFrame()
st.write("# Twitter data scraping")
option = st.selectbox('How would you like the data to be searched?',('Keyword', 'Hashtag'))
word = st.text_input('Please enter a '+option, 'Enter text here')
start = st.date_input("Select the start date", datetime.date(2022, 1, 1),key='d1')
end = st.date_input("Select the end date", datetime.date(2023, 1, 1),key='d2')
tweet_c = st.slider('How many tweets to scrape', 0, 1000, 5)
tweets_list = []

# SCRAPE DATA USING TwitterSearchScraper
if word:
    try:
        if option=='Keyword':
            for i,tweet in enumerate(sntwitter.TwitterSearchScraper(f'{word} lang:en since:{start} until:{end}').get_items()):
                if i>tweet_c-1:
                    break
                tweets_list.append([ tweet.content, tweet.user.username, tweet.replyCount, tweet.retweetCount,tweet.likeCount ])
            tweets_df = pd.DataFrame(tweets_list, columns=['Content', 'Username', 'ReplyCount', 'RetweetCount', 'LikeCount'])
        else:
            for i,tweet in enumerate(sntwitter.TwitterHashtagScraper(f'{word} lang:en since:{start} until:{end}').get_items()):
                if i>tweet_c-1:
                    break            
                tweets_list.append([ tweet.content, tweet.user.username, tweet.replyCount, tweet.retweetCount,tweet.likeCount ])
            tweets_df = pd.DataFrame(tweets_list, columns=['Content', 'Username', 'ReplyCount', 'RetweetCount', 'LikeCount'])
    except Exception as e:
        st.error(f"Too many requests, TwitterRateLimit exceeded, please try again after few hours")
        st.stop()

else:
    st.warning(option,' cant be empty', icon="⚠️")

def get_database():
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb+srv://maity97:1234@cluster0.ar2njph.mongodb.net/?retryWrites=true&w=majority"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['tweet']


# DOWNLOAD AS CSV
@st.cache_data # IMPORTANT: Cache the conversion to prevent computation on every rerun
def convert_df(df):    
    return df.to_csv().encode('utf-8')

if not tweets_df.empty:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        csv = convert_df(tweets_df) # CSV
        c=st.download_button(label="Download data as CSV",data=csv,file_name='Twitter_data.csv',mime='text/csv',)        
    with col2:    # JSON
        json_string = tweets_df.to_json(orient ='records')
        j=st.download_button(label="Download data as JSON",file_name="Twitter_data.json",mime="application/json",data=json_string,)

    with col3:    # JSON
        db = get_database()
        collection = db['tweet']
        tweets_df.reset_index(inplace=True)
        data_dict = tweets_df.to_dict("records")
        collection.insert_many(data_dict)
        jj=st.button(label="Download to mongodb",)

    with col4: # SHOW
        y=st.button('Show Tweets',key=2)

if c:
    st.success("The Scraped Data is Downloaded as .CSV file:",icon="✅")  
if j:
    st.success("The Scraped Data is Downloaded as .JSON file",icon="✅")     
if jj:
    st.success("The Scraped Data is Downloaded to mondo db",icon="✅")     

#if x: # DISPLAY
#    st.success("The Scraped Data is:",icon="✅")
#    st.write(tweets_df)
if y: # DISPLAY
    
    st.success("Tweets Scraped Successfully:",icon="✅")
    st.write(tweets_df)

    


            


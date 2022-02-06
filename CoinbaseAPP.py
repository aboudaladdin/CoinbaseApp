# -*- coding: utf-8 -*-
"""
Created on Fri Jan 28 11:55:12 2022 EG

@author: Abdelrahman Arafa
@description: Maintains an in-memory panda dataframe  
            (for a corresponding set of cryptos) 
            consisting of the past 30 seconds of data..
"""

# Imports
import numpy as np
import pandas as pd
import cbpro
import time
from datetime import datetime, timedelta

import plotly.express as plx
import plotly.graph_objects as go
import dash
from dash import dcc
 
from dash import html
from dash.dependencies import Input, Output 

'''
## Coinbase API

We cannot work with public-private endpoint the minimum granularity is 1 min
[ref https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductcandles]

For our task we need real-time data at least per second.
also there is a rate limit to poll information from such public-private endpoints
[ref https://docs.cloud.coinbase.com/exchange/docs/rate-limits]

instead we will use websockets and listen from its stream which is realtime and publicly available
[ref https://docs.cloud.coinbase.com/exchange/docs/overview]
'''

'''
Class IAWebsocketClient
'''
class IAWebsocketClient(cbpro.WebsocketClient):
    '''
    Describtion : 
        WebSocketClient to handle Incoming messages from Coinbase
        * inherits from cbpro WebsocketClient class.
        * we are using the ticker channel
        * connecting to "wss://ws-feed.pro.coinbase.com/"
        * storing the products list and all incoming ticks
        * cleaning up the dataframe by cleanup_period of seconds.
    '''
    
    def __init__(self,products):
        super(IAWebsocketClient,self).__init__(channels = ['ticker'])
        self.url = "wss://ws-feed.pro.coinbase.com/"
        self.products = products
        self.ticksDF = None 
        self.cleanup_period = 100 # seconds
        self.last_cleanup = time.time()
        
    def on_open(self):
        self.message_count = 0
        print("Listening..!")

    def on_message(self, msg):
        '''
        Handles input stream from Coinbase WS
            * We need product id, timestamp and price
            * for each tick we recieve we send it to be processed
        '''
        self.message_count += 1
        # check if we are recieving tick prices send to processing
        if msg['type'] == 'ticker':
            self.process_ticks(msg)
        elif msg['type'] == 'error':
            print(msg['message'])
            
        # add other incoming msg processing here
    
    '''
    Main Processing of Ticker messages
    '''
    def process_ticks(self, msg):
        '''
        Description :
        ------
        Append incoming ticks to a dataframe directly on the form of {Time, Product, Price, Volume, Ticks}
            * Volume is {last size}
            * Ticks is calculated later in one aggregation call
        Discard old ticks by self.cleanup_period seconds
        ''' 
        
        dt = datetime.fromisoformat(msg["time"][:-1])
        product_id = msg['product_id']
        price = float(msg['price'])
        tick_volume = float(msg['last_size'])
        index = msg['sequence']
        
        row = {'time': dt, 'product_id': product_id, 'price': price, 'volume': tick_volume, 'ticks':0}
        row_df = pd.DataFrame(row, index=[index])
        
        if self.ticksDF is None:
            self.ticksDF = row_df
        else:
            self.ticksDF = pd.concat(objs=[self.ticksDF, row_df],axis=0)
            
            # handle cleaning up
            if (time.time() - self.last_cleanup) > self.cleanup_period:
                self.last_cleanup = time.time()
                self.clean_up()
        
    def clean_up(self):
        '''
        Description :
        ------
        Discard old ticks > 30 seconds
            * should be lazy updated (not with every tick)
        ''' 
        self.ticksDF = self.ticksDF[self.ticksDF['time'] > (self.ticksDF['time'].iloc[-1] - timedelta(seconds=30))]
    
    def get_dataframe(self,product_id):
        '''
        Description :
        ---------
        Resample current dataframe and send the aggregated live candle data for a specific product_id
        Using Pandas builtin time-series functions
        [reference] : [https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html]
            * we use aggregations to calculate volumes and ticks
            * please note we use forward filling for gaps in between data (Nans) but there are other options too
                *[reference] : [https://pandas.pydata.org/docs/reference/resampling.html#upsampling]
            * if there is no product_id yet, returns None
        '''
        if self.ticksDF is not None:
            df_pid = self.ticksDF[self.ticksDF['product_id'] == product_id]
            
            if df_pid is not None:
                df_pid = df_pid.resample('1S', on='time').agg({'price':'ohlc','volume':'sum','ticks':'size'}).ffill()
                
                df_pid = df_pid.iloc[-30:] # process only the last 30 seconds
                
                df_pid.columns = df_pid.columns.droplevel(0) # for better access to column names
                
                # for ffill with last closing price instead of using a loop, 
                # we use vectorized pandas builtin functions
                df_pid[['open','high','low']] = np.where(df_pid[['ticks']] == 0, 
                                                         df_pid[['close']],df_pid[['open','high','low']])
                return df_pid 
        return None
    
    def on_close(self):
        print("End Listening!")


'''
    Initial Content (Plotly)
'''

# # we can append to the list multiple prodcuts to the stream
product_list = ['BTC-USD', 'ETH-USD', 'ADA-USD', 'DOGE-USD', 
                 'SHIB-USD', 'SOL-USD', 'ICP-USD', 'BCH-USD']

fig = go.Figure()
fig.update_layout(xaxis_rangeslider_visible=False, title=f'Live Signal',
        yaxis_title='BTC-USD',
        plot_bgcolor = '#f5f5f5',
 
)
fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#efefef')
fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#efefef')

'''
    App layout and Interactivity Dash
'''
app = dash.Dash(external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"])

graph_config = dict({'scrollZoom': False,'responsive': False})
app.layout = html.Div(children=[
                            html.H1("Live Signal Demonstration"),
                            dcc.Dropdown(id='product_id',options=[{"label": x, "value": x} for x in product_list],
                                         value=product_list[0]),
                            dcc.Graph(id='live-chart',figure=fig, style={'width': '97vw', 'height': '95vh'}, config=graph_config),
                            dcc.Interval(id='graph-update',interval=100) 
                            ]) 

#_________________________________ 
#_________________________________
if __name__ == '__main__':

    '''
    Starting the stream
    '''

    wsClient = IAWebsocketClient(product_list)
    wsClient.start()
    time.sleep(0.5)

    '''
    Update graph via events
    '''
    @app.callback(
    Output('live-chart','figure'),
    Input('graph-update', 'n_intervals'),
    Input(component_id='product_id', component_property='value')
    )
    def graph_update(n, product_id):
        global wsClient
        global fig
        msg_count = wsClient.message_count

        df = wsClient.get_dataframe(product_id)
 
        if df is not None:
            fig.update(data=[go.Candlestick(open=df['open'],high=df['high'],low=df['low'],close=df['close'],x=df.index ) ])
            fig.update_layout(yaxis_title= product_id , title=f'{product_id} Real-time signal, Total Ticks {msg_count}')
            return fig
            
        return go.Figure()
    
    '''
    Stream Data to dash server
    '''
    app.run_server(debug=False, port = 8070)   

    wsClient.close()
    # end stream
#_________________________________
#_________________________________
 







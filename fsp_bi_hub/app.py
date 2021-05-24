from flask import Flask, render_template, request, flash

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import subfunctions.contacts_grab as contacts_grab
import subfunctions.deals_grab as deals_grab
# from subfunctions.data_tests import run_tests
import numpy as np
import pandas as pd
import os
from time import sleep

app = Flask(__name__)
app.config.update(
    SECRET_KEY = os.environ['SECRET_KEY']
)

apikey = os.environ['hapikey']

@app.route("/",methods=['GET','POST'])
@app.route("/home",methods=['GET','POST'])
def home():
    if request.method == 'POST':
        if request.form.get('contacts') == 'contacts':
            try:
                contacts_grab.run_grab()
                flash('Contacts have been updated!', 'success')
            except:
                flash('Contact upload failed.', 'failure')
    return render_template('home.html',hapikey = apikey)





if __name__ == "__main__":
    app.run(debug=False)

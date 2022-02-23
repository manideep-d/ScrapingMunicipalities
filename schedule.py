import pytz
import requests
import subprocess
from apscheduler.schedulers.twisted import TwistedScheduler
from twisted.internet import reactor

def send_request():
    requests.post("https://scraping-municipalities.herokuapp.com/schedule.json", data={
        "project": "scraping_websites",
        "spider": "MunicipalitiesSpider"
    })

if __name__ == "__main__":
    subprocess.run("scrapyd-deploy", shell=True, universal_newlines=True)
    scheduler = TwistedScheduler(timezone=pytz.timezone('US/Eastern'))
    # cron trigger that schedules jobxs
    scheduler.add_job(send_request, 'cron', year=2022, month=2, day=23, hour=19)
    # start the scheduler
    scheduler.start()
    reactor.run()
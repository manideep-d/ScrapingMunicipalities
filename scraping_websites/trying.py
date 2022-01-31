import tldextract

ext = tldextract.extract('https://smartcity.mississauga.ca/wp-content/uploads/2021/10/74-Reasons-Why-Mississsauga-is-an-Intelligent-Community.pdf')

print(ext.domain)
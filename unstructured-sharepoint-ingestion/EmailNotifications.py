import requests
import json
tenantId = 'ea80952e-a476-42d4-aaf4-5457852b0f7e'
fromAddress = 'gusiplogapp@bp.com'
clientId = dbutils.secrets.get(scope='ZNEUDL1P40INGing-kvl00', key='DPBS-DE-EMAIL-SPID-KV')
clientSecret = dbutils.secrets.get(scope='ZNEUDL1P40INGing-kvl00', key='DPBS-DE-EMAIL-SPPWD-KV')

tokenUrl = 'https://login.microsoftonline.com/' + tenantId + '/oauth2/token'
url = 'https://graph.microsoft.com/v1.0/users/' +fromAddress+ '/sendMail'
def get_access_token(tokenUrl, clientId, clientSecret):
  '''
  Generates & returns the access token from a token URL, client ID, and client secret.
  '''
  payload = {
  'grant_type' : 'client_credentials',
  'resource' : 'https://graph.microsoft.com',
  'client_id' : clientId  ,
  'client_secret' : clientSecret
 }
  response = requests.post(url=tokenUrl, data=payload)
  accessToken = response.json()['access_token']
  return accessToken
  
accessToken = get_access_token(tokenUrl, clientId, clientSecret)
def sendEmail(toEmailList, subject, message):
  toRecipients = []
  try:
    accessToken = get_access_token(tokenUrl, clientId, clientSecret)
    for email in toEmailList:
      toRecipients.append({"emailAddress": {"address": email}})
    
    headers = {
      'Accept' : 'application/json',
      'Content-Type': 'application/json',
      'Authorization' : 'Bearer ' + accessToken
    }

    payload = json.dumps({
      "message": {
        "subject": subject,
        "body": {
          "contentType": "HTML",
          "content": message
        },
        "toRecipients": toRecipients
      },
      "saveToSentItems": "false"
    })

    response = requests.post(url=url, headers= headers, data=payload)
    if response.status_code == 202:
      print('Job notification email sent to ' + ', '.join((email) for email in toEmailList ))
    else:
      print('Send email request failed with: '+str(response.status_code))
      
  except Exception as e:
    print('Exception occurred in sendEmail : {}'.format(str(e)))

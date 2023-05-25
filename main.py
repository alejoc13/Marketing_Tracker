import helper.workflow as wf
import configparser

config = configparser.ConfigParser()
config.read('marketing.config')

if __name__ =='__main__':
    TOKEN = config.get('DEFAULT','TOKEN')
    wf.filteringData(TOKEN)
    

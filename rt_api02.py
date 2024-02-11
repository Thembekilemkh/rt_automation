import requests, pickle, datetime
from datetime import datetime as dt
import csv, gzip, os, sys, json, urllib

from requests import post
from threading import Thread
from queue import Queue
from time import sleep
import time

# Third party libraries
import pandas as pd

host = '********'
user = '*********'
pass_ = "*********"
token = 'token'

cwd = os.getcwd()

class RTManager():
    def __init__(self, **kwargs):
        self.ticket_db = {}
        self.host = kwargs['host']
        self.user = kwargs['user']
        self.pass_ = kwargs['pass_']
        self.token = kwargs['token']
        self.home_dir = kwargs['home_dir']
        self.file = kwargs['file']
        self.cookie = kwargs['cookie']

    def get_ticket_details(self, **kwargs):
        # Get reqquired variables
        ticket_id = kwargs["ticket_id"]
        j = kwargs['j']
        taskQ = kwargs['taskQ']

        url = f"https://{self.host}/REST/1.0/search/ticket?query=Status=" + "'__Active__' AND 'CF.{Escalate to Client}'= 'Yes'"
        url = f"https://{self.host}/REST/1.0/ticket/ticket_id/show"
        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': self.cookie
        }
        payload={}

        break_ = False
        got_details = False
        url = url.replace('ticket_id', ticket_id)
        try:
            response = requests.request("GET", url, headers=headers, data=payload, verify=False)
            got_details = True
        except Exception as e:
            pass

        if got_details:
            self.ticket_db[ticket_id] = {}
            details = (response.text).split('\n')

            i = 0
            for detail in details:
                if i >= 2:
                    if len(detail.split(':')) < 2:
                        pass
                    else:
                        field = detail.split(':')[0]
                        value = detail.split(':')[1]
                        self.ticket_db[ticket_id][field] = value
                    # print(f"Field: {field}")
                i = i+1

            if j == len(details):
                break_ = True

            j = j+1

            task = taskQ.get()
            print(f"\n\n{task} done!!\n\n")
            return break_, j

    def get_tickets_url(self, **kwargs):
        url = kwargs['url']

        headers = {
            'Authorization': f'Basic {token}',
            'Cookie': f'{self.cookie}'
        }
        payload={}
        ticket_db = {}
        got_ticket = False
        self.ticket_db = {}

        # Get tickets
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        tickets = (response.text).split('\n')

        # Format ticket into a python object
        i = 0
        for ticket in tickets:
            if i >= 2:
                if len(ticket.split(':')) < 2:
                    pass
                else:
                    ticket_num = ticket.split(':')[0]
                    ticket_db[ticket_num] = {}
                    got_ticket = True
            i = i+1
        ticket_numbers = list(ticket_db.keys())
        print(f"\n\ntickets: {ticket_db}\n\n")
        ticket_numbers['break']

    def get_tickets(self):
        url = f"""https://{self.host}/REST/1.0/search/ticket?query=Status="""+"""'__Active__' AND Queue='Internal' 'CF.{Escalate to Client}' : 'Yes'"""
        url = f"https://{self.host}/REST/1.0/search/ticket?query=Status=" + "'__Active__' AND 'CF.{Escalate to Client}'= 'Yes'"
        url_details =  f"https://{self.host}/REST/1.0/ticket/ticket_id/show"

        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': f'{self.cookie}'
        }
        payload={}
        ticket_db = {}
        got_ticket = False
        self.ticket_db = {}

        # Get tickets
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        tickets = (response.text).split('\n')

        # Format ticket into a python object
        i = 0
        for ticket in tickets:
            if i >= 2:
                if len(ticket.split(':')) < 2:
                    pass
                else:
                    ticket_num = ticket.split(':')[0]
                    ticket_db[ticket_num] = {}
                    got_ticket = True
            i = i+1
        ticket_numbers = list(ticket_db.keys())

        # Create CSV
        cols = ["ticket number"]
        rows = []
        for num in ticket_numbers:
            rows.append([num])

        with open('ticket_numbers.csv', 'w') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)
            write.writerow(cols)

            write.writerows(rows)

        # Get ticket details
        if len(list(ticket_db.keys())):
            j = 0
            taskQ = Queue()
            for key, val in ticket_db.items():
                ticket_num = key

                taskQ.put(f"Task {j+1}")
                Thread(target=self.get_ticket_details, kwargs={"ticket_id":ticket_num, "taskQ":taskQ, "j":j}).start()
                # break_ = constantQ.get()
                # j = constantQ.get()

                if taskQ.qsize() >= 30:
                    pass
                # if break_:
                #     break
                j = j + 1

            # wait for all ticket details to be collected before continuing
            all_tasks_done = False
            while all_tasks_done == False:
                if taskQ.empty():
                    all_tasks_done = True
                else:
                    pass
                    # print("Waiting 30 seconds for more details to be collected from ticket data...")
                    # sleep(30)

            print("We got all tickets")
            # Create a csv with the relevant fields
        print(ticket_db)

        return ticket_numbers

    def editTicket(self, ticketID, ticketProperties):
        """Accepts ticket ID and ticket properties dictionary as input"""

        content = ""
        credentials = {'user': self.user, 'pass': self.pass_}
        for key, value in ticketProperties.items():
            content += key + ": " + value + "\n"
        encoded = "content=" + urllib.parse.quote_plus(content)
        r = post(f"https://{self.host}/REST/1.0/ticket/{ticketID}/edit", params=credentials,
                    data=encoded, verify = False)
        return r

    def get_only_the_ticket_history(self, id_='5755054', status='Resolved', queue='Internal', **kwargs):
        # Get required data
        url_history =  f"https://{self.host}/REST/1.0/ticket/ticket_id/history"
        url_transaction = f"https://{self.host}/REST/1.0/ticket/ticket_id/get_transaction"
        tickets=kwargs['tickets']
        start=kwargs["start"]
        history_file=kwargs["history_file"]
        checkpoint=kwargs["checkpoint"]
        data = {"tickets":[],
                "history":[],
                "transactions":[]}

        # prepare request
        payload={}

        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': f'{self.cookie}'
        }

        # Save current checkpoint
        with open(checkpoint, "r") as file:
            checkpoint_ticket = file.read()
            checkpoint_ticket = checkpoint_ticket.replace("\n", "")

        # get history
        i = 1
        start_now = False
        prev_ticket = ""
        for ticket_num in tickets:
            ticket_num = ticket_num.replace("\n", "")

            # ready to start
            if ticket_num == checkpoint_ticket:
                start_now = True

            if start_now and prev_ticket != ticket_num:
                # Place a request
                ticket_num = ticket_num.replace("\n", "")

                # URL's
                temp_url1 = url_history.replace('ticket_id', ticket_num)
                temp_url2 = url_transaction.replace('ticket_id', ticket_num)

                # Requests
                response1 = requests.request("GET", temp_url1, headers=headers, data=payload, verify=False)
                response2 = requests.request("GET", temp_url2, headers=headers, data=payload, verify=False)

                # Formatting
                history = list((response1.text).split('\n'))
                transaction = list((response2.text).split('\n'))

                # Save the data to a file
                if history[0] == "RT/4.4.3 200 Ok":
                    data["tickets"].append(ticket_num)
                    data["history"].append(history[1:])
                    data["transactions"].append(transaction)


                    with open(history_file, "wb") as pickle_out:
                        pickle.dump(data, pickle_out)


                    # Save current checkpoint
                    with open(checkpoint, "w") as file:
                        file.write(str(ticket_num))

                # show timelapse
                end = time.time()
                print(f"\n\n{str(i)}: {ticket_num}\t timelapse: {str((end-start)* 10**3)}ms\n{history[4:]}\n\n")

            prev_ticket = ticket_num
            i=i+1

    def history_id(self, **kwargs):
        # Get required variables
        history_id = str(69430788)
        ticket = str(5897519)
        url_history_id_url =  f"https://{self.host}/REST/1.0/ticket/ticket_id/history/id/history_id"

        # prepare request
        payload={}

        headers = {
            'Authorization': f'Basic {token}',
            'Cookie': f'{self.cookie}'
        }


        try:
            # Get history data
            temp_url = url_history_id_url.replace('ticket_id', str(ticket))
            temp_url = temp_url.replace('history_id', str(history_id))
            response = requests.request("GET", temp_url, headers=headers, data=payload, verify=False)
            history = (response.text).split('\n')

            print(history)

        except Exception as e:
            pass


    def get_history_id(self, **kwargs):
        # Get required data
        cwd = os.getcwd()
        url_history_id_url =  f"https://{self.host}/REST/1.0/ticket/ticket_id/history/id/history_id"
        tickets_csv =  f"{self.home_dir}\\{self.file}"
        new_file = f"{self.home_dir}\\{self.file}"

        # prepare request
        payload={}

        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': self.cookie
        }

        # get tickets
        tickets_df = pd.read_csv(tickets_csv, delimiter=",")
        tickets_df = tickets_df[['ticket', 'created', 'history', 'workstation_name']]

        # Add the new columns
        tickets_df["assigned"] = ""
        tickets_df["escalated"] = ""
        tickets_df["resolved"] = ""
        collected_tickets = []
        loaded = 0

        # refined df
        refined_tickets = pd.DataFrame()

        # Repair and replace
        for j in range(len(tickets_df)):
            history = str(tickets_df['history'].iloc[j]).split(",")
            ticket = tickets_df['ticket'].iloc[j]

            if str(ticket) in collected_tickets:
                pass
            else:
                # add ticket to inventory
                collected_tickets.append(str(ticket))
                collect_assigned = False
                collect_escalated = False
                collect_resolved = False
                found_ticket = False
                for msg in history:
                    if ":" in msg:
                        # get history id
                        msg_parts = msg.split(":")
                        history_id = msg_parts[0].replace("'","").replace('"', "")
                        found_ticket = True

                        # Find relevant messages (created, Assigned, resolved)
                        if "new" in msg_parts[1] and "assigned" in msg_parts[1]:
                            # Ticket has been assigned
                            collect_assigned = True

                        elif "Escalate to Client Yes added" in msg_parts[1]:
                            # Ticket has been escalated
                            collect_escalated = True

                        elif "assigned" in msg_parts[1] and "resolved" in msg_parts[1]:
                            # Ticket is now resolved after escalation
                            collect_resolved = True

                        if collect_assigned or collect_escalated or collect_resolved:
                            # Get history data
                            temp_url = url_history_id_url.replace('ticket_id', str(ticket))
                            temp_url = temp_url.replace('history_id', str(history_id))
                            response = requests.request("GET", temp_url, headers=headers, data=payload, verify=False)
                            history_ = (response.text).split('\n')
                            # print(f"\n\nHistory: {ticket}: {history}\n\n\n")

                            for deets in history_:
                                if "Created:" in deets:
                                    created = deets[9:]

                            if collect_assigned:
                                tickets_df.loc[j, 'assigned'] = created
                                collect_assigned = False

                            elif collect_escalated:
                                tickets_df.loc[j, 'escalated'] = created
                                collect_escalated = False

                            elif collect_resolved:
                                tickets_df.loc[j, 'resolved'] = created
                                collect_resolved = False

                # load data to df
                if found_ticket:
                    found_ticket = False
                    if loaded >= 1:

                        refined_tickets = refined_tickets.append(tickets_df.iloc[j], ignore_index=True)
                        loaded = loaded+1
                    else:
                        data = [[tickets_df['ticket'].iloc[j], tickets_df['created'].iloc[j], tickets_df['assigned'].iloc[j], tickets_df['escalated'].iloc[j], tickets_df['resolved'].iloc[j], tickets_df['history'].iloc[j]]]
                        refined_tickets = pd.DataFrame(data, columns=['ticket', "created", 'assigned', 'escalated', "resolved", "history"])
                        loaded = loaded+1

                    # Turn off collect switch
            print(f"Dealt with {str(j)}")

            # load new df to output folder
            refined_tickets.to_csv(new_file, index=False)
        print("\n\nDF has been created\n\n")

    def get_ticket_history(self, id_='5741816', status='Resolved', queue='Internal', **kwargs):
        # Get required data
        url = f"https://{self.host}/REST/1.0/search/ticket?query=Status='{status}' AND Queue='{queue}' AND id='{id_}'"
        url_history =  f"https://{self.host}/REST/1.0/ticket/ticket_id/history"

        # prepare request
        payload={}

        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': f'{self.cookie}'
        }

        ticket_db = {}
        got_ticket = False

        # Get tickets
        print(url)
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        tickets = (response.text).split('\n')

        # clean received tickets
        i = 0
        for ticket in tickets:
            if i >= 2:
                if len(ticket.split(':')) < 2:
                    pass
                else:
                    ticket_num = ticket.split(':')[0]
                    ticket_db[ticket_num] = {}
                    got_ticket = True
                    print(ticket_num)
            i = i+1

        # get history
        if len(list(ticket_db.keys())):
            j = 0
            for key, val in ticket_db.items():
                ticket_num = key
                ticket_detail = val
                url_history = url_history.replace('ticket_id', key)
                response = requests.request("GET", url_history, headers=headers, data=payload, verify=False)
                history = (response.text).split('\n')
                print(f"\n\nHistory: {ticket_num}: {history}\n\n\n")
                break

    def search_ticket_by_name(self, subject='', id_='', status='__Active__', queue='Internal', **kwargs):
        # Get required data
        if subject != '':
            url = f"https://{self.host}/REST/1.0/search/ticket?query=Status='{status}' AND Queue='{queue}' AND Subject LIKE '{subject}'"
        elif id_ != '':
            url = f"https://{self.host}/REST/1.0/search/ticket?query=Status='{status}' AND Queue='{queue}' AND id='{id_}'"

        url_details =  f"https://{self.host}/REST/1.0/ticket/ticket_id/show"
        # url_details =  f"https://{self.host}/REST/1.0/ticket/ticket_id/history"
        payload={}

        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': f'{self.cookie}'
        }

        ticket_db = {}
        got_ticket = False

        # Get tickets
        print(url)
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        tickets = (response.text).split('\n')
        i = 0
        for ticket in tickets:
            if i >= 2:
                if len(ticket.split(':')) < 2:
                    pass
                else:
                    ticket_num = ticket.split(':')[0]
                    ticket_db[ticket_num] = {}
                    got_ticket = True
                    # print(ticket_num)
            i = i+1

        # Get ticket details
        if len(list(ticket_db.keys())):
            j = 0
            for key, val in ticket_db.items():
                ticket_num = key
                ticket_detail = val
                url_details = url_details.replace('ticket_id', key)
                response = requests.request("GET", url_details, headers=headers, data=payload, verify=False)
                details = (response.text).split('\n')

                i = 0
                for detail in details:
                    if i >= 2:
                        if len(detail.split(':')) < 2:
                            pass
                        else:
                            field = detail.split(':')[0]
                            value = detail.split(':')[1]
                            ticket_db[key][field] = value
                        # print(f"Field: {field}")
                    i = i+1
                if j == len(details):
                    break

                j = j+1

        if got_ticket:
            return ticket_num, ticket_db[ticket_num]
        else:
            return None, None

    def create_splunk_search(self):
        file = f"{os.getcwd()}\\Data source manipulation\\Clients\\Decommed_Implats.csv"
        splunk_query = "| inputlookup data_source_profile.csv\n| eval notes=if("
        suffix = ', "172.20.8.200", host_ip)\n\n| outputlookup data_source_profile.csv'

        # reading the csv file
        df = pd.read_csv(f"{file}")

        i = 0
        # print(df.keys())
        for i in range(len(df)):
            if (df['parsing'].iloc[i] == 'T') or ('dec-' not in df['dsname'].iloc[i].lower()):
                splunk_query = f"""{splunk_query} host="{df['dsname'].iloc[i]}" OR"""


        splunk_query = splunk_query[:-3]+suffix

        input(f'\nRun the following spunk search to to update the receiver IP:\n\n\n{splunk_query}\n\nOnce you have run the script press enter to complete.\n\n')

    def edit_multiple(self, **kwargs):
        # Required variables
        updated_num = 0
        failed_num = 0
        failed = []
        all_updated = True
        new_value = kwargs['new_value']
        field = kwargs['field']
        ticket_nums = []

        # Get tickets from search
        # url = kwargs['url']
        # ticket_nums = self.get_tickets_url(url=url)

        with open(f'{os.getcwd()}\\calltickets.txt') as f:
            lines = f.readlines()
            # print(lines)
            for line in lines:
                ticket_nums.append(line.split('\n')[0])

        # Organize filed to be updated
        assigned_properties = {field:new_value}

        # Update ticket
        for i in range(len(ticket_nums)):
            # Get the ticket number
            ticket_num = ticket_nums[i]
            if ticket_num is not None:

                try:
                    result = self.editTicket(ticket_num, assigned_properties)
                    print(f"\n\nResult text: {result.text}")
                    print(f"\n\nResult content: {result.content}\n\n")

                    updated_num = updated_num+1
                except Exception as e:
                    failed_num = failed_num+1
                    failed.append(ticket_num)
                    all_updated = False
                    print(f'\n\nFailed to print one on the results: {e}\n\n')

                ticket_nums['break']


            else:
                pass

        return all_updated, f'In total {str(len(ticket_nums))} were meant to be updated.\n{str(updated_num)} tickets have been updated. \n{str(failed_num)} tickets have failed to update.\n Here is the list of failed updates {failed}'

    def get_tickets_within_timerange(self, queue='Internal', **kwargs):
        # Get required data
        status = kwargs['status']
        created = kwargs['created']
        severity = kwargs['severity']
        exclusion_list = kwargs['exclusion_list']
        ticket_db = {}

        # Fix URL
        base_url = f"https://{self.host}/REST/1.0/search/ticket?query=Status='{status}' AND Queue='{queue}' AND Created > '{created}'"#AND Created > '2023-09-08' Created > '2023-08-27' AND Created < '2023-08-29'  AND 'CF.{{Severity}}' = '1'

        # Fix request data
        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': self.cookie
        }
        credentials = {'user': self.user, 'pass': self.paas_}

        payload={}

        # Get tickets
        response = requests.request("GET", base_url, data=payload, verify=False, params=credentials)#headers=headers
        tickets = (response.text).split('\n')

        # Filter str
        lin_rsa_src = "Netwitness Event Source Monitoring Sample"
        win_rsa_src = "Netwitness Windows Agent Not Sending Logs Sample"

        i = 0
        for ticket in tickets:
            if i >= 2:
                if len(ticket.split(':')) < 2:
                    pass
                else:
                    subject = ticket.split(':')[1]

                    if (win_rsa_src not in subject or lin_rsa_src not in subject):
                        include = True
                        for x in exclusion_list:
                            if x in subject:
                                include = False
                        if include:
                            ticket_num = ticket.split(':')[0]
                            ticket_db[ticket_num] = {}
                            got_ticket = True
            i = i+1

        return ticket_db

    def get_details(self, **kwargs):
        # Get required data
        ticket_db = kwargs['tickets_db']
        ticket_db_new = {}

        # Prepare URL
        url = f"https://{self.host}/REST/1.0/ticket/ticket_id/show"

        # Prepare request parameters
        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': self.cookie
        }
        credentials = {'user': self.user, 'pass': self.paas_}

        payload={}
        found_ticket = False

        # Kill switch
        die_now = False

        clients = []

        # Loop through tickets and get details
        for key, val in ticket_db.items():
            ticket_num = key

            temp_url = url.replace("ticket_id", str(ticket_num))

            # Make request
            response = requests.request("GET", temp_url, data=payload, verify=False, params=credentials)
            details = (response.text).split('\n')

            # Filter for mmh tickets
            for deet in details:
                if "Queue" in deet:
                    if ("mmh" in str(deet).lower()):
                        print(f"Found ticket: {details}")
                        found_ticket = True
                    elif ("internal" in str(deet).lower()):
                        for det in details:
                            if "CF.{Client}" in det:
                                if ("mmh" in str(deet).lower()):
                                    old_clients = clients
                                    clients, client = self.check_collected_clients(deet, clients)
                                    found_ticket = True
                                    print(f"Found ticket: {details}")
                                else:
                                    break



            # Collect the data point we want
            if found_ticket:
                ticket_db_new[ticket_num]  = {}
                # die_now = True
                i = 0
                for detail in details:
                    if ":" in detail:
                        field = detail.split(':')[0]
                        value = detail.split(':')[1]
                        if i >= 2:
                            if len(detail.split(':')) < 2:
                                pass
                            else:
                                if "Cc" in field or "id" in field:
                                    if "id" in field:
                                        ticket_db_new[ticket_num][field] = value.replace("ticket/", "")
                                    elif "Created" in field:
                                        mins = detail.split(":")[2]
                                        secs = detail.split(":")[3]
                                        value = f"{value}:{mins}:{secs}"

                                        created_date = dt.strptime(value, " %a %b %d %H:%M:%S %Y")
                                        created_date = created_date.strftime("%Y/%m/%d %H:%M:%S")
                                        ticket_db_new[ticket_num][field] = created_date
                                    else:
                                        field = field.replace("CF.{", "").replace("}", "")
                                        ticket_db_new[ticket_num][field] = value
                            # print(f"Field: {field}")
                    i = i+1
                found_ticket = False

            # if die_now:
                #  break

        print(f"MMH source: {ticket_db_new}")
        return ticket_db_new

    def get_history(self, **kwargs):
        # Get required data
        ticket = kwargs['ticket']

        # Get url
        url_history =  f"https://{self.host}/REST/1.0/ticket/ticket_id/history"

        # prepare request
        payload={}

        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': self.cookie
        }
        credentials = {'user': self.user, 'pass': self.paas_}

        # Send a request
        temp_url = url_history.replace('ticket_id', ticket)
        response1 = requests.request("GET", temp_url, data=payload, verify=False, params=credentials)

        history = list((response1.text).split('\n'))

        if history[0] == "RT/4.4.3 200 Ok":
            history = history[4:]

        return history

    def get_history_details(self, **kwargs):
        # Get required data
        history = kwargs['history']
        ticket = kwargs['ticket']
        url_history_id_url =  f"https://{self.host}/REST/1.0/ticket/ticket_id/history/id/history_id"
        assigned = ""
        escalated = ""
        resolved = ""

        # prepare request
        payload={}

        headers = {
            'Authorization': f'Basic {self.token}',
            'Cookie': self.cookie
        }
        credentials = {'user': self.user, 'pass': self.paas_}

        # add ticket to inventory
        collect_assigned = False
        collect_escalated = False
        collect_resolved = False
        for msg in history:
            if ":" in msg:
                # get history id
                msg_parts = msg.split(":")
                history_id = msg_parts[0].replace("'","").replace('"', "")

                # Find relevant messages (created, Assigned, resolved)
                if "new" in msg_parts[1] and "assigned" in msg_parts[1]:
                    # Ticket has been assigned
                    collect_assigned = True

                elif "Escalate to Client Yes added" in msg_parts[1] or "Escalate to Client No changed to Yes" in msg_parts[1]:
                    # Ticket has been escalated
                    collect_escalated = True

                elif "assigned" in msg_parts[1] and "resolved" in msg_parts[1]:
                    # Ticket is now resolved after escalation
                    collect_resolved = True

                if collect_assigned or collect_escalated or collect_resolved:
                    # Get history data
                    temp_url = url_history_id_url.replace('ticket_id', str(ticket))
                    temp_url = temp_url.replace('history_id', str(history_id))
                    response = requests.request("GET", temp_url, data=payload, verify=False, params=credentials)
                    history_ = (response.text).split('\n')
                    created=False

                    for deets in history_:
                        if "Created:" in deets:
                            created = deets[9:]

                    if created:
                        if collect_assigned:
                            assigned = created

                            assigned = dt.strptime(str(assigned), "%Y-%m-%d %H:%M:%S")
                            assigned = assigned + datetime.timedelta(hours=2)

                            collect_assigned = False

                        elif collect_escalated:
                            escalated = created
                            collect_escalated = False

                            escalated = dt.strptime(str(escalated), "%Y-%m-%d %H:%M:%S")
                            escalated = escalated + datetime.timedelta(hours=2)

                        elif collect_resolved:
                            resolved = created
                            collect_resolved = False

                            resolved = dt.strptime(str(resolved), "%Y-%m-%d %H:%M:%S")
                            resolved = resolved + datetime.timedelta(hours=2)

        return assigned, escalated, resolved

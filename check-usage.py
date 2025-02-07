#!/usr/bin/env python3
import argparse
import calendar
import datetime
import getpass
import json
import os
import sys
import socket
import requests
from urllib.parse import urlencode

VERSION = 3.0
DEBUG = False  # Toggle debug output

# mode from hostname
MODE_MYBRC = 'mybrc'
MODE_MYLRC = 'mylrc'
MODE = MODE_MYBRC if 'brc' in socket.gethostname() else MODE_MYLRC

SUPPORT_TEAM = 'BRC' if MODE == MODE_MYBRC else 'LRC'
SUPPORT_EMAIL = 'brc-hpc-help@berkeley.edu' if MODE == MODE_MYBRC else 'hpcshelp@lbl.gov'

# timestamp formats
timestamp_format_complete = '%Y-%m-%dT%H:%M:%S'
timestamp_format_minimal = '%Y-%m-%d'

# api endpoints
BASE_URL = f"https://{'mybrc.brc.berkeley.edu' if MODE == MODE_MYBRC else 'mylrc.lbl.gov'}/api/"
ALLOCATION_ENDPOINT = BASE_URL + 'allocations/'
ALLOCATION_USERS_ENDPOINT = BASE_URL + 'allocation_users/'
JOB_ENDPOINT = BASE_URL + 'jobs/'

# compute resources lookup
COMPUTE_RESOURCES_TABLE = {
    MODE_MYBRC: {
        'ac': 'Savio Compute',
        'co': 'Savio Compute',
        'fc': 'Savio Compute',
        'ic': 'Savio Compute',
        'pc': 'Savio Compute',
        'vector': 'Vector Compute',
        'abc': 'ABC Compute',
    },
    MODE_MYLRC: {
        'ac': 'LAWRENCIUM Compute',
        'lr': 'LAWRENCIUM Compute',
        'pc': 'LAWRENCIUM Compute',
    }
}

if DEBUG:
    BASE_URL = 'http://scgup-dev.lbl.gov/api/' if MODE == MODE_MYBRC else 'http://scgup-dev.lbl.gov:8443/api/'
    ALLOCATION_ENDPOINT = BASE_URL + 'allocations/'
    ALLOCATION_USERS_ENDPOINT = BASE_URL + 'allocation_users/'
    JOB_ENDPOINT = BASE_URL + 'jobs/'


# authentication token from configuration file. config file in /allhands is readable by anyone, idk if thats a security issue
CONFIG_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           f'check_usage_{MODE}.conf')
if not os.path.exists(CONFIG_FILE):
    print(f'Config file {CONFIG_FILE} missing...')
    sys.exit(1)

with open(CONFIG_FILE, 'r') as f:
    AUTH_TOKEN = f.read().strip()

HEADERS = {"Authorization": AUTH_TOKEN}

# utility functions from old script

def red_str(text: str) -> str:
    return f"\033[91m{text}\033[00m"

def green_str(text: str) -> str:
    return f"\033[92m{text}\033[00m"

def yellow_str(text: str) -> str:
    return f"\033[93m{text}\033[00m"

def check_valid_date(s: str) -> str:
    """Validate that a date string matches an expected format"""
    try:
        datetime.datetime.strptime(s, timestamp_format_complete)
        return s
    except ValueError:
        try:
            datetime.datetime.strptime(s, timestamp_format_minimal)
            return s
        except ValueError:
            raise argparse.ArgumentTypeError(f"Invalid time specification: {s}")

def parse_datetime(dt_str: str) -> datetime.datetime:
    """Parse a datetime string using the complete or minimal format"""
    try:
        return datetime.datetime.strptime(dt_str, timestamp_format_complete)
    except ValueError:
        return datetime.datetime.strptime(dt_str, timestamp_format_minimal)

def to_timestamp(dt_str: str, to_utc: bool = False) -> float:
    """Convert a datetime string to a UTC timestamp"""
    dt_obj = parse_datetime(dt_str)
    # assume naive datetimes are in UTC
    return dt_obj.replace(tzinfo=datetime.timezone.utc).timestamp()

def to_timestring(timestamp: float) -> str:
    """Convert a UTC timestamp to an ISO 8601 string with 'Z' suffix"""
    dt_obj = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
    return dt_obj.strftime(timestamp_format_complete) + 'Z'

def paginate_requests(url: str, params: dict) -> list:
    """Fetch all pages from a paginated API endpoint"""
    results = []
    page = 1
    while True:
        params_with_page = params.copy()
        params_with_page['page'] = page
        try:
            resp = requests.get(url, headers=HEADERS, params=params_with_page)
            resp.raise_for_status()
            response = resp.json()
        except Exception as e:
            if DEBUG:
                print(f"[paginate_requests] Error: {e}")
            break

        if response.get('results'):
            results.extend(response['results'])
        else:
            break

        if not response.get('next'):
            break
        page += 1
    return results

def single_request(url: str, params: dict = None) -> list:
    """Fetch a single requests results from the API"""
    try:
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        response = resp.json()
        return response.get('results', [])
    except Exception as e:
        if DEBUG:
            print(f"[single_request] Error: {e}")
        return []

def get_project_start(project: str) -> str:
    """Retrieve the start date for a project from the allocations endpoint"""
    header = project.split('_')[0]
    compute_resources = COMPUTE_RESOURCES_TABLE.get(MODE, {}).get(header,
                           f"{header.upper()} Compute")
    params = {'project': project, 'resources': compute_resources}
    response = single_request(ALLOCATION_ENDPOINT, params)
    if not response:
        if DEBUG:
            print(f"[get_project_start] Project not found: {project}")
        return None
    try:
        creation = response[0]['start_date']
        return creation.split('.')[0] if '.' in creation else creation
    except Exception as e:
        if DEBUG:
            print(f"[get_project_start] Error: {e}")
        print(f"ERR: Information missing in {SUPPORT_TEAM} database. "
              f"Contact {SUPPORT_EMAIL} if the problem persists.")
        sys.exit(1)

def get_cpu_usage(user: str = None, account: str = None):
    """Retrieve CPU usage information based on the query parameters"""
    params = {'start_time': START_TIME, 'end_time': END_TIME}
    if user:
        params['user'] = user
    if account:
        params['account'] = account
    try:
        resp = requests.get(JOB_ENDPOINT, headers=HEADERS, params=params)
        resp.raise_for_status()
        response = resp.json()
    except Exception as e:
        if DEBUG:
            print(f"[get_cpu_usage] Error: {e}")
        if user and not account:
            return -1, -1, -1
        response = {'count': 0, 'total_cpu_time': 0, 'total_amount': 0}

    job_count = response.get('count', 0)
    total_cpu = response.get('total_cpu_time', 0)
    total_amount = response.get('total_amount', 0)
    return job_count, total_cpu, total_amount

def process_account_query(account: str, expand: bool, default_start_used: bool):
    header = account.split('_')[0]
    compute_resources = COMPUTE_RESOURCES_TABLE.get(MODE, {}).get(header,
                           f"{header.upper()} Compute")
    params = {'project': account, 'resources': compute_resources}
    response = single_request(ALLOCATION_ENDPOINT, params)
    if not response:
        if DEBUG:
            print(f"[process_account_query] Account not found: {account}")
        print(f"ERR: Account not found: {account}")
        return

    allocation_id = response[0]['id']
    allocation_url = f"{ALLOCATION_ENDPOINT}{allocation_id}/attributes/"
    response_attr = single_request(allocation_url, {'type': 'Service Units'})
    if not response_attr:
        if DEBUG:
            print("[process_account_query] Error: No attribute found.")
        print(f"ERR: Backend error, contact {SUPPORT_TEAM} Support ({SUPPORT_EMAIL}).")
        sys.exit(1)

    try:
        allocation = int(float(response_attr[0]['value']))
    except Exception as e:
        if DEBUG:
            print(f"[process_account_query] Error converting allocation: {e}")
        allocation = None

    if account.startswith('ac_') or account.startswith('co_'):
        try:
            account_usage = response_attr[0]['usage']['value']
        except KeyError:
            print(f"ERR: Backend error, contact {SUPPORT_TEAM} Support ({SUPPORT_EMAIL}).")
            sys.exit(1)
        job_count, cpu_usage, _ = get_cpu_usage(account=account)
    else:
        job_count, cpu_usage, account_usage = get_cpu_usage(account=account)

    header_msg = f"Usage for ACCOUNT {account} [{START_TIME_READABLE}, {END_TIME_READABLE}]:"
    if not default_start_used:
        print(f"{header_msg} {job_count} jobs, {cpu_usage:.2f} CPUHrs, {account_usage} SUs.")
    else:
        print(f"{header_msg} {job_count} jobs, {cpu_usage:.2f} CPUHrs, "
              f"{account_usage} SUs used from an allocation of {allocation} SUs.")

    if expand:
        user_list = paginate_requests(ALLOCATION_USERS_ENDPOINT, {'project': account})
        for user_item in user_list:
            if not user_item.get('user'):
                continue
            user_name = user_item['user']
            user_jobs, user_cpu, user_usage = get_cpu_usage(user_name, account)
            try:
                percentage = (float(user_usage) / float(account_usage)) * 100 if float(account_usage) else 0.0
            except Exception:
                percentage = 0.0

            if percentage < 75:
                percentage_str = green_str(f"{percentage:.2f}")
            elif percentage > 100:
                percentage_str = red_str(f"{percentage:.2f}")
            else:
                percentage_str = yellow_str(f"{percentage:.2f}")
            print(f"\tUsage for USER {user_name} in ACCOUNT {account} "
                  f"[{START_TIME_READABLE}, {END_TIME_READABLE}]: {user_jobs} jobs, {user_cpu:.2f} CPUHrs, "
                  f"{user_usage} ({percentage_str}%) SUs.")

def process_user_query(user: str, expand: bool):
    total_jobs, total_cpu, total_usage = get_cpu_usage(user=user)
    if total_jobs == total_cpu == total_usage == -1:
        print(f"ERR: User not found: {user}")
        return

    header_msg = f"Usage for USER {user} [{START_TIME_READABLE}, {END_TIME_READABLE}]:"
    print(f"{header_msg} {total_jobs} jobs, {total_cpu:.2f} CPUHrs, {total_usage} SUs used.")

    if expand:
        response = paginate_requests(ALLOCATION_USERS_ENDPOINT, {'user': user})
        for allocation in response:
            allocation_account = allocation.get('project')
            allocation_jobs, allocation_cpu, allocation_usage = get_cpu_usage(user=user,
                                                                             account=allocation_account)
            prefix = "\t"
            if allocation.get('status') == 'Removed':
                prefix += "(User removed from account) "
            print(prefix + f"Usage for USER {user} in ACCOUNT {allocation_account} "
                  f"[{START_TIME_READABLE}, {END_TIME_READABLE}]: {allocation_jobs} jobs, {allocation_cpu:.2f} CPUHrs, "
                  f"{allocation_usage} SUs.")

def main():
    global START_TIME, END_TIME, START_TIME_READABLE, END_TIME_READABLE  # readable is ugly

    parser = argparse.ArgumentParser(description=f"[version: {VERSION}]")
    parser.add_argument('-u', dest='user', help='Check usage of this user')
    parser.add_argument('-a', dest='account', help='Check usage of this account')
    parser.add_argument('-E', dest='expand', action='store_true',
                        help='Expand user/account usage')
    parser.add_argument('-s', dest='start', type=check_valid_date,
                        help='Start time (YYYY-MM-DD[THH:MM:SS])')
    parser.add_argument('-e', dest='end', type=check_valid_date,
                        help='End time (YYYY-MM-DD[THH:MM:SS])')
    args = parser.parse_args()

    user = args.user
    account = args.account
    expand = args.expand

    # default start date based on the current time and mode.
    now = datetime.datetime.now(datetime.timezone.utc)
    current_month = now.month
    current_year = now.year
    break_month = '06' if MODE == MODE_MYBRC else '10'
    year = current_year if current_month >= int(break_month) else (current_year - 1)
    default_start = f"{year}-{break_month}-01T00:00:00"

    start_arg = args.start if args.start else default_start
    end_arg = args.end if args.end else now.strftime(timestamp_format_complete)

    default_start_used = (start_arg == default_start)
    # if an account is provided and the default start is used, adjust start based on project info. carried from old script
    if default_start_used and account:
        project_start = get_project_start(account)
        if project_start:
            start_arg = project_start

    # conver dates to UTC timestamps and then to strings. Also carried from old script
    START_TIME = str(int(to_timestamp(start_arg, to_utc=True)))
    END_TIME = str(int(to_timestamp(end_arg, to_utc=True)))

    START_TIME_READABLE = to_timestring(to_timestamp(start_arg, to_utc=True))
    END_TIME_READABLE = to_timestring(to_timestamp(end_arg, to_utc=True))

    # if no user or account is provided, default to the current user
    if not user and not account:
        user = getpass.getuser()

    # validate that start time is not after end time
    if to_timestamp(start_arg, to_utc=True) > to_timestamp(end_arg, to_utc=True):
        print(f"ERR: Start time ({START_TIME}) is after end time ({END_TIME}).")
        sys.exit(1)

    if to_timestamp("2020-06-01", to_utc=True) > to_timestamp(start_arg, to_utc=True):
        print(f"INFO: Information might be inaccurate; for accurate information contact "
              f"{SUPPORT_TEAM} support ({SUPPORT_EMAIL}).")

    if user:
        process_user_query(user, expand)
    if account:
        if account.startswith('ac_'):
            print("INFO: Start Date shown may be inaccurate.")
        process_account_query(account, expand, default_start_used)

if __name__ == "__main__":
    main()

import subprocess
import os
import json
from json.decoder import JSONDecoder
from tqdm import tqdm
import datetime
import sqlite3

def create_db(conn):
    # Create a cursor object
    c = conn.cursor()

    # Create tables
    c.execute('''
        CREATE TABLE File (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL
        );
    ''')

    c.execute('''
        CREATE TABLE Commits (
            commit_hash TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            author TEXT NOT NULL,
            message TEXT,
            previous_commit TEXT,
            FOREIGN KEY(previous_commit) REFERENCES Commits(commit_hash)
        );
    ''')

    c.execute('''
        CREATE TABLE Refactoring (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commit_hash TEXT NOT NULL,
            refactoring_type TEXT NOT NULL,
            details TEXT,
            FOREIGN KEY(commit_hash) REFERENCES Commits(commit_hash)
        );
    ''')

    c.execute('''
        CREATE TABLE RefactoredFile (
            refactoringId INTEGER NOT NULL,
            fileId INTEGER NOT NULL,
            FOREIGN KEY(refactoringId) REFERENCES Refactoring(id),
            FOREIGN KEY(fileId) REFERENCES File(id)
        );
    ''')

    c.execute('''
        CREATE TABLE OrganicMetric (
            id INTEGER PRIMARY KEY,
            metric_type TEXT NOT NULL,
            file INTEGER NOT NULL,
            method_name TEXT,
            value REAL,
            commit_hash TEXT NOT NULL,
            FOREIGN KEY(file) REFERENCES File(id),
            FOREIGN KEY(commit_hash) REFERENCES Commits(commit_hash)
        );
    ''')

    c.execute('''
        CREATE TABLE OrganicSmell (
            id INTEGER PRIMARY KEY,
            file INTEGER NOT NULL,
            commit_hash TEXT NOT NULL,
            smell TEXT NOT NULL,
            FOREIGN KEY(file) REFERENCES File(id),
            FOREIGN KEY(commit_hash) REFERENCES Commits(commit_hash)
        );
    ''')

    c.execute('''
        CREATE TABLE CodeChurn (
            id INTEGER PRIMARY KEY,
            commit_hash TEXT NOT NULL,
            file_path TEXT NOT NULL,
            additions INTEGER NOT NULL,
            deletions INTEGER NOT NULL,
            FOREIGN KEY(commit_hash) REFERENCES Commits(commit_hash)
        );
    ''')

    # Commit the changes and close the connection
    conn.commit()

def get_commit_details(commit_hash):
    result = subprocess.run(["git", "show", "-s", "--format=%H\t%ct\t%an\t%s", commit_hash], capture_output=True, text=True)
    details = result.stdout.strip().split('\t', 3)  # Split the output into four parts
    return {
        'commit_hash': details[0],
        'commit_timestamp': details[1],
        'commit_author': details[2],
        'commit_message': details[3]
    }

def insert_commit(conn, commit_hash, timestamp, author, message, previous_commit):
    c = conn.cursor()
    c.execute('''
        INSERT INTO Commits (commit_hash, timestamp, author, message, previous_commit)
        VALUES (?, ?, ?, ?, ?)
    ''', (commit_hash, timestamp, author, message, previous_commit))
    conn.commit()

def insert_file(conn, file_path):
    c = conn.cursor()
    c.execute('''
        INSERT INTO File (path)
        VALUES (?)
    ''', (file_path,))
    conn.commit()
    return c.lastrowid

def insert_refactoring(conn, commit_hash, refactoring_type, details):
    c = conn.cursor()
    c.execute('''
        INSERT INTO Refactoring (commit_hash, refactoring_type, details)
        VALUES (?, ?, ?)
    ''', (commit_hash, refactoring_type, details))
    conn.commit()
    return c.lastrowid

def insert_refactored_file(conn, refactoring_id, file_id):
    c = conn.cursor()
    c.execute('''
        INSERT INTO RefactoredFile (refactoringId, fileId)
        VALUES (?, ?)
    ''', (refactoring_id, file_id))
    conn.commit()

def insert_organic_smell(conn, file_id, commit_hash, smell):
    c = conn.cursor()
    c.execute('''
        INSERT INTO OrganicSmell (file, commit_hash, smell)
        VALUES (?, ?, ?)
    ''', (file_id, commit_hash, smell))
    conn.commit()

def insert_code_churn(conn, commit_hash, file_path, additions, deletions):
    c = conn.cursor()
    c.execute('''
        INSERT INTO CodeChurn (commit_hash, file_path, additions, deletions)
        VALUES (?, ?, ?, ?)
    ''', (commit_hash, file_path, additions, deletions))
    conn.commit()

def insert_organic_metric(conn, metric_type, file_id, method_name, value, commit_hash):
    c = conn.cursor()
    c.execute('''
        INSERT INTO OrganicMetric (metric_type, file, method_name, value, commit_hash)
        VALUES (?, ?, ?, ?, ?)
    ''', (metric_type, file_id, method_name, value, commit_hash))
    conn.commit()


def get_code_churn(commit_hash, file_path):
    # Checkout to the specific commit hash
    subprocess.run(["git", "checkout", commit_hash, '--force'])

    # Get the date two weeks before the commit date
    commit_date = subprocess.run(["git", "show", "-s", "--format=%ci", commit_hash], capture_output=True).stdout.decode().strip()
    commit_date = datetime.datetime.strptime(commit_date, "%Y-%m-%d %H:%M:%S %z")
    two_weeks_before = (commit_date - datetime.timedelta(weeks=2)).strftime("%Y-%m-%d")

    # Run git log with --numstat and --since options
    result = subprocess.run(["git", "log", "--numstat", "--since=" + two_weeks_before, '--pretty=format:', "--", file_path], capture_output=True).stdout.decode()

    # Parse the result to get additions and deletions
    additions = 0
    deletions = 0
    for line in result.split("\n"):
        if line:
            add, delete, _ = line.split("\t")
            additions += int(add)
            deletions += int(delete)

    return additions, deletions

def get_code_metrics(json_file, filenames):
    # Load the JSON file
    with open(json_file) as f:
        data = json.load(f)

    # Initialize a list to store the code metrics
    code_metrics = []

    # Iterate over the data
    for item in data:
        # Check if the item has 'metrics' and 'sourceFile' keys
        if 'metricsValues' in item and 'sourceFile' in item:
            # Check if the file name is in the list
            if item['sourceFile']['fileRelativePath'] in filenames:
                # Iterate over the metrics
                for metric, value in item['metricsValues'].items():
                    # Append the metric type, method name, and value to the list
                    code_metrics.append((metric, item['sourceFile']['fileRelativePath'], value, None))

        # Check if the item has 'methods' key
        if 'methods' in item:
            # Iterate over the methods
            for method in item['methods']:
                # Check if the method has 'metrics' key
                if 'metricsValues' in method:
                    # Check if the file name is in the list
                    if item['sourceFile']['fileRelativePath'] in filenames:
                        # Iterate over the metrics
                        for metric, value in method['metricsValues'].items():
                            # Append the metric type, method name, and value to the list
                            qualifiedName = method['fullyQualifiedName']
                            if (qualifiedName is None):
                              qualifiedName = ''

                            code_metrics.append((metric, item['sourceFile']['fileRelativePath'], value, qualifiedName.split('.')[-1]))

    return code_metrics

def get_code_smells(json_file, filenames):
    # Load the JSON file
    with open(json_file) as f:
        data = json.load(f)

    # Initialize a list to store the code smells
    code_smells = []

    # Iterate over the data
    for item in data:
        # Check if the item has 'smells' and 'sourceFile' keys
        if 'smells' in item and 'sourceFile' in item:
            # Check if the file name is in the list
            if item['sourceFile']['fileRelativePath'] in filenames:
                # Iterate over the smells
                for smell in item['smells']:
                    # Append the smell name and file path to the list
                    code_smells.append((smell['name'], item['sourceFile']['fileRelativePath']))

        # Check if the item has 'methods' key
        if 'methods' in item:
            # Iterate over the methods
            for method in item['methods']:
                # Check if the method has 'smells' key
                if 'smells' in method:
                    # Check if the file name is in the list
                    if item['sourceFile']['fileRelativePath'] in filenames:
                        # Iterate over the smells
                        for smell in method['smells']:
                            # Append the smell name and file path to the list
                            code_smells.append((smell['name'], item['sourceFile']['fileRelativePath']))

    return code_smells

file_ids = {}

def get_unique_file_names(json_object):
    # Initialize a set to store the unique file names
    unique_file_names = set()

    # Iterate over the leftSideLocations and rightSideLocations arrays
    for location in json_object['leftSideLocations'] + json_object['rightSideLocations']:
        # Add the filePath value to the set
        unique_file_names.add(location['filePath'])

    # insert the files into the database and collect the ids into a map
    for file in unique_file_names:
        if file not in file_ids:
            file_ids[file] = insert_file(conn, file)

    return list(unique_file_names)

# 1. Gather user input
git_url = input("Enter the Git repository URL: ")
repo_name = input("Enter the repository name: ")
start_commit = input("Enter the start commit hash: ")
end_commit = input("Enter the end commit hash: ")

old_dir = os.getcwd()

conn = sqlite3.connect('refactoring.db')
create_db(conn)

print('running RefactoringMiner...')
# 2. Run RefactoringMiner.jar
subprocess.run(["java", "-jar", "RefactoringMiner.jar", git_url, repo_name, start_commit, end_commit])

# 3. Process refactoring output
refactorings = []
commit_refactorings = {}
output_dir = f"tmp/output/{repo_name}"
print(f"Processing refactoring output from {output_dir}")


for filename in tqdm(os.listdir(output_dir)):
    if filename.endswith(".json"):
        commit_hash = filename.split(".")[0]

        if commit_hash == '':
            pass

        commit_refactorings[commit_hash] = []


        with open(os.path.join(output_dir, filename)) as f:
            # Initialize an empty string for accumulated JSON chunks
            chunk = ""

            # Iterate through each character in the file
            for char in f.read():
                chunk += char
                try:
                    # Attempt to decode the accumulated chunk
                    obj = json.loads(chunk)
                    ref = {}
                    ref['commit'] = commit_hash
                    ref['refactoring_type'] = obj['type']
                    ref['details'] = str(obj)
                    ref['files'] = get_unique_file_names(obj)
                    refactoring_id = insert_refactoring(conn, ref['commit'], ref['refactoring_type'], ref['details'])
                    for file in ref['files']:
                        file_id = file_ids[file]
                        insert_refactored_file(conn, refactoring_id, file_id)
                    refactorings.append(ref)
                    commit_refactorings[commit_hash].append(ref)
                    chunk = ""  # Reset chunk for the next object
                except json.JSONDecodeError:
                    pass

# 4. Run organic-v0.1.1-OPT.jar for each commit
smells_data = {}
metrics_data = {}
commit_map = {}  # Dictionary to map commit to previous commit hash
churn_data = {}

refactoring_commits = list(commit_refactorings.keys())

print('Running organic and gathering churn data...')

os.mkdir(f"tmp/output/smells")

seen_commits = set()

for commit_hash in tqdm(refactoring_commits):

    os.chdir(f"tmp/{repo_name}")
    # Checkout the commit (current and previous)
    subprocess.run(["git", "checkout", commit_hash, '--force'], capture_output=True)
    previous_commit = subprocess.run(["git", "rev-parse", "HEAD~1"], capture_output=True).stdout.decode().strip()

    if commit_hash == '' or previous_commit == '':
        print('blank commit hash found!!!')
        os.chdir(old_dir)
        continue

    if commit_hash in seen_commits or previous_commit in seen_commits:
        os.chdir(old_dir)
        continue

    previous_commit_details = get_commit_details(previous_commit)
    insert_commit(conn, previous_commit, previous_commit_details['commit_timestamp'], previous_commit_details['commit_author'], previous_commit_details['commit_message'], None)

    current_commit_details = get_commit_details(commit_hash)
    insert_commit(conn, commit_hash, current_commit_details['commit_timestamp'], current_commit_details['commit_author'], current_commit_details['commit_message'], previous_commit)

    os.chdir(old_dir)

    # Run organic on both commits
    for commit in [commit_hash, previous_commit]:
        os.chdir(f"tmp/{repo_name}")
        subprocess.run(["git", "checkout", commit, '--force'], capture_output=True)

        os.chdir(old_dir)
        subprocess.run(["java", "-jar", "organic-v0.1.1-OPT.jar", "-sf", f"tmp/output/smells/{repo_name}-{commit}.json", "-src", f"tmp/{repo_name}"])

        refactored_files = []
        if commit in commit_refactorings:
            refactored_files = [(f"tmp/{repo_name}/" + a) for a in ref['files'] for ref in commit_refactorings[commit]]

        smells_data[commit] = get_code_smells(os.path.join('tmp/output/smells/', f"{repo_name}-{commit}.json"), refactored_files)
        metrics_data[commit] = get_code_metrics(os.path.join('tmp/output/smells/', f"{repo_name}-{commit}.json"), refactored_files)
        for smell in smells_data[commit]:
            insert_organic_smell(conn, file_ids[smell[1].replace(f"tmp/{repo_name}/", '')], commit, smell[0])

        for metric in metrics_data[commit]:
            insert_organic_metric(conn, metric[0], file_ids[metric[1].replace(f"tmp/{repo_name}/", '')], metric[3], metric[2], commit)



    os.chdir(f"tmp/{repo_name}")
    # Reset to the current commit after analysis
    subprocess.run(["git", "checkout", commit_hash, '--force'], capture_output=True)

    seen_commits.add(commit_hash)
    seen_commits.add(previous_commit)


    # Get the code churn for each refactored file
    churn_data[commit_hash] = {}
    for ref in commit_refactorings[commit_hash]:
        for file in ref['files']:
            additions, deletions = get_code_churn(commit_hash, file)
            churn_data[commit_hash][file] = (additions, deletions)
            insert_code_churn(conn, commit_hash, file_ids[file.replace(f"tmp/{repo_name}/", '')], additions, deletions)

    os.chdir(old_dir)



conn.close()

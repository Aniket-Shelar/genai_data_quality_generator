import os
from django.shortcuts import render
from django.http import HttpResponse
from databricks import sql
from databricks.sdk import WorkspaceClient
import json
import time
from .forms import RulesApprovalForm
from django.shortcuts import redirect
from dotenv import load_dotenv
# Create your views here.


load_dotenv()

def get_rules():
    with sql.connect(
            server_hostname = os.getenv("DBR_HOSTNAME"),
            http_path = os.getenv("DBR_HTTP_PATH"),
            access_token = os.getenv("DBR_TOKEN")
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM dq_rules.rules LIMIT 10")
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                for col in ['created_at', 'table_name', 'table']:
                    columns.remove(col) 
                rows = []
                for rec in result:
                    row = []
                    row.append(rec.confidence)
                    row.append(rec.description)
                    row.append(rec.rule_id)
                    row.append(rec.rule_sql)
                    row.append(rec.rule_type)
                    row.append(rec.severity)
                    row.append(rec.status)
                    rows.append(row)
                print("ROWS: ", rows)
                return {'rows': rows, 'columns': columns}

def get_clusters(request):
    w = WorkspaceClient(
    host  = os.getenv("DBR_HOSTNAME"),
    token = os.getenv("DBR_TOKEN")
    )
    cluster_names = []
    for c in w.clusters.list():
         cluster_names.append(c.cluster_name)

    return HttpResponse(f"Clusters: {cluster_names}")

def run_job(request):
    
    w = WorkspaceClient(
    host  = os.getenv("DBR_HOSTNAME"),
    token = os.getenv("DBR_TOKEN"),
    )
    
    # Replace with your Job ID
    job_id = 121021789409425

    # Trigger the job
    run = w.jobs.run_now(
         job_id=job_id, 
         notebook_params=
         {"job_code": "C101"}
        )
    run_id = run.run_id

    seconds = 0
    while True:
        state = w.jobs.get_run(run_id).state.life_cycle_state
        print(f"{type(state)}: Running since {seconds} seconds")
        if state in ("RunLifeCycleState.TERMINATED", "RunLifeCycleState.SKIPPED", "RunLifeCycleState.INTERNAL_ERROR"):
            break
        time.sleep(5)
        seconds += 5
    print("RUNID", run_id)
    # Fetch notebook output
    
    # output = w.jobs.get_run_output(run_id)

    # output.notebook_output.result contains the dbutils.notebook.exit() value
    # result_str = output.notebook_output.result

    # result = json.loads(result_str)

    # print("Notebook result:", result)

    return HttpResponse(f"Run completed: {run_id}")

def home_view(request):
    context = {}
    return render(request, "home_links.html", context=context)

def approve_rules(request):

    if request.method == 'POST':
        form = RulesApprovalForm(request.POST)
        if form.is_valid():
            rule_data = get_rules()
            print("RULE DATA:", rule_data)
            # form.save()
            # return redirect('home_view')  
    else:
        rule_data = {}
        form = RulesApprovalForm()
    return render(request, 'approve_rules.html', {'form': form, 'rule_data': rule_data})
        
    

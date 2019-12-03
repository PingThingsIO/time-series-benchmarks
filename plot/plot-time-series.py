import argparse
import json
import matplotlib.pyplot as plt
import pandas as pd
import os
import re
import sys

def list_to_str(elements):
    list_str = ""
    num_elements = len(elements)
    if num_elements == 0:
        return list_str
    elif num_elements == 1:
        return elements[0]
    elif num_elements == 2:
        return elements[0] + " and " + elements[1]
    else:
        for element in range(0, len(elements) - 1):
            list_str = list_str + elements[element] + ", "
        list_str = list_str + "and " + elements[element + 1]
    return list_str

# Parse the arguments
parser = argparse.ArgumentParser(description='')
parser.add_argument('--file', action='append')

# Loop through all the files provided
files = parser.parse_args().file
# Create directory for plot images
if not os.path.exists('plots'):
    os.makedirs('plots')
# Create a new markdown file
output_file = open("output.mkd", "w")
parameters = []
metrics = ["FullParallelInsert.MPPS", "Insert.MPPS", "FullParallelSequentialRead.MPPS",
             "SequentialRead.MPPS", "SingleStreamSequentialRead.MPPS", "StreamCreation"]
colors = ["r", "b", "g", "c", "m", "k"]
data_list = []
if files == None:
    print("No files provided")
    sys.exit(0)
elif len(files) > 6:
    print("Only the first six files provided will be compared for simplicity")
    files = files[0:6]
for file in files:
    # Store the contents of each file into a dataframe
    try:
        json_file = open(file)
    except FileNotFoundError:
        print("File not found")
        sys.exit(0)
    try:
        data = json.load(json_file)
    except json.decoder.JSONDecodeError:
        print("The provided file is not a JSON")
        sys.exit(0)
    #print("File Name: " + file + "\n" + str(data["fingerprint"]))
    pattern = re.compile(r"^report-(\S+)-pretty.json$")
    match = pattern.match(file)
    if match == None:
        pattern = re.compile(r"^report-(\S+).json$")
        match = pattern.match(file)
        if match == None:
            print("Please provide valid files of the format 'report-<name>.json")
            sys.exit(0)
    # Loop through the benchmarks
    for test in data["benchmarks"]:
        # Loop through the parameter sets for each benchmark
        for instance in data["benchmarks"][test]:
            data_dict = {"Database":match.group(1)}
            data_dict.update({"Benchmark":test})
            # Loop through each parameter
            parameter_str = ""
            for parameter in instance["parameters"]:
                try:
                    parameters.index(parameter)
                except ValueError:
                    parameters.append(parameter)
                parameter_str = parameter_str + parameter + "=" + instance["parameters"][parameter] + " "
            data_dict.update({"parameters":parameter_str})
            data_dict.update(instance["results"])
            data_list.append(data_dict)
df = pd.DataFrame(data_list)

output_file.write("# Plots Comparing Various Metrics Over Different Databases\n")
# Loop through the different metrics (Duration, MPPS...)
for metric in range(0, len(metrics)):
    plt.clf()
    ax = plt.subplot()
    databases = df["Database"].dropna().unique()
    counter = 0
    ticks = []
    tick_labels = []
    for database in range(0, len(databases)):
        groups = df.groupby(["Database", "parameters"]).mean()[metrics[metric]].dropna()[databases[database]]
        counter = 0
        combinations = groups.keys()
        for row in combinations:
            x = 0.6 + database * 0.4 + counter * 2
            if database == 0:
                parameters_used = row.split(" ")
                if len(parameters_used) < 1:
                    pass
                elif len(parameters_used) == 2:
                    ticks.append(x - 0.1 + len(databases) * 0.2)
                    tick_labels.append(parameters_used[0])
                elif len(parameters_used) > 2:
                    ticks.append(x - 0.1 + len(databases) * 0.2)
                    par_str = ""
                    for par in parameters_used:
                        par_str = par_str + par + "\n"
                    tick_labels.append(par_str)
            ax.barh(x, width=groups[row], height=0.4, align='center', color=colors[database]\
                , label=databases[database] if counter == 0 else "")
            counter = counter + 1
    # Style for plots
    ax.set_xlabel(metrics[metric])
    ax.set_ylabel("Parameter Set")
    ax.set_title(metrics[metric] + " Across Different Parameters")
    ax.set_yticks(ticks)
    ax.set_yticklabels(tick_labels)
    ax.yaxis.set_tick_params(labelsize=4)
    ax.margins(x=0.3)
    ax.legend(loc='best', prop={'size': 6})
    plt.autoscale(True)
    plt.tight_layout()
    try:
        os.remove("plots/" + metrics[metric] + ".png")
    except FileNotFoundError:
        pass
    plt.savefig("plots/" + metrics[metric] + ".png")
    print("plots/" + metrics[metric] + ".png" + " created")
    # Write a markdown file to provide context to the plots
    output_file.write("### In the below plot, " + metrics[metric] + \
    " (shown along the x axis) is compared across the databases: " + list_to_str(databases) + \
        ". The parameters: "  + list_to_str(parameters) + " were controlled for and are given along the y axis.\n")
    output_file.write("![" + metrics[metric] + " plot](plots/" + metrics[metric] + ".png)\n")
output_file.close()
print("output.mkd created")
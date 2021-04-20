import os
import json
import shlex
import atexit
import argparse
import subprocess
from tqdm import tqdm
import concurrent.futures as cf

parser = argparse.ArgumentParser(description='Download data from discord')
parser.add_argument('-config', type=str, default=None,
                    help='config json', required=True)
parser.add_argument('-out', type=str, default="./out",
                    help='output dir')
parser.add_argument('-workers', type=int, default=8,
                    help='number of workers to spawn')
parser.add_argument('-partition', type=int, default=300000,
                    help='number of messages per file')
args = parser.parse_args()

assert args.config.endswith(".json"), "must be a json file"
config=json.load(open(args.config, "r"))

#config format:
"""
{
    "token":"123456758975432134567897654321",
    "channels": [1,2,3,4,5,6, ... ]
}
"""

existing=json.load(open("existing.txt", "r"))

try:
    os.mkdir("dce")
    subprocess.call(shlex.split("wget -nH --cut-dirs 6 https://github.com/Tyrrrz/DiscordChatExporter/releases/download/2.26.1/DiscordChatExporter.CLI.zip"))
    subprocess.call(shlex.split("unzip DiscordChatExporter.CLI.zip -d dce"))
    subprocess.call(shlex.split("rm DiscordChatExporter.CLI.zip"))
except: pass

arguments=f"""dotnet dce/DiscordChatExporter.Cli.dll export \
  -t {config['token']} \
  -f json -p {args.partition} \
  --dateformat yyy-mm-dd \
  -o {os.path.join(args.out, "data")} \
  -c """

def gen_dir(dir):
    try:os.mkdir(dir)
    except: pass

def write_out():
    json.dump(existing,open("existing.txt", "w"))
    
atexit.register(write_out)

def call_proc(channel):
    if type(channel) == list:
        gen_dir(channel[1])
        cmd = arguments.replace(os.path.join(args.out, "data"), os.path.join(args.out, channel[1]))+str(channel[0])
        channel = channel[0]
    else:
        cmd=arguments+str(channel)
    if int(channel) in existing: return (channel, "Error.", "Error: Channel has already been downloaded")
    existing.append(channel)
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return (channel, out.decode().strip().replace("\n", " - "), err.decode())

with cf.ThreadPoolExecutor(max_workers = args.workers) as executor:
    for result in tqdm(executor.map(call_proc, config["channels"]), total=len(config["channels"]), desc=f"Exporting channels in {args.config}"):
        tqdm.write(f"{result[0]} ||| out: {result[1]}\n{'err: '+result[2] if result[2] else ''}")
#!/usr/bin/env python
import os,os.path
import yaml
from collections import defaultdict
import logging
logger = logging.getLogger(__name__)
import subprocess

def createDirIfNeeded(path):
    if not os.path.exists(path):
        os.makedirs(path)
    if not os.path.isdir(path):
        raise RuntimeError("Output {0} is not a directory!".format(args.output))

def listFiles(pattern, opts=""):
    res = subprocess.check_output(["dasgoclient", "-query", "file dataset={0} {1}".format(pattern, opts)])
    return [ ln.strip() for ln in res.split("\n") if ln.strip() ]

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Produce lists of LFNs to transfer, and local path lists for bamboo")
    parser.add_argument("--year", help="Data-taking year")
    parser.add_argument("-i", "--input", required=True, help="TopNanoAOD datasets file")
    parser.add_argument("processes", nargs="+", help="List of processes (or files with processes)")
    parser.add_argument("--dbs", default="prod/phys03", help="DBS instance to query") 
    parser.add_argument("-o", "--output", help="Directory to write outputs to")
    parser.add_argument("--siteinfo", required=True, help="YAML file with site information")
    parser.add_argument("--dest", required=True, help="Destination directory")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")
    parser.add_argument("--homesite", help="Current site (exclude for transfers)")
    parser.add_argument("--doTransfers", action="store_true", help="Also launch the transfers")
    args = parser.parse_args()
    print(args)
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    qryOpts = ""
    if args.dbs != "prod/global":
        qryOpts = " instance={0}".format(args.dbs)
    ## get list of selected processes
    selProcs = set()
    for procArg in args.processes:
        if os.path.isfile(procArg):
            with open(procArg) as pFile:
                for ln in pFile:
                    if ln.strip() and not ln.startswith("#"):
                        selProcs.add(ln.strip())
        else:
            selProcs.add(procArg)
    ## now get the full configs for these
    with open(args.input) as ymlF:
        topNanoAODs = yaml.load(ymlF, yaml.SafeLoader)
    topNanoAODs = dict((smpNm, smpCfg) for smpNm, smpCfg in topNanoAODs[args.year].items() if smpNm in selProcs)
    prefixes = defaultdict(list)
    if topNanoAODs:
        with open(args.siteinfo) as siF:
            siteInfo = yaml.load(siF, yaml.SafeLoader)
        usersite_prefixes = defaultdict(list)
        usersite_filelists = defaultdict(list)
        ## also sort by user and site, to organise the transfers
        lfnDir = os.path.join(args.output, "LFNs")
        locfnDir = os.path.join(args.output, "files")
        createDirIfNeeded(lfnDir)
        createDirIfNeeded(locfnDir)
        for smpName, smpConfig in topNanoAODs.items():
            dsFiles = listFiles(smpConfig["dbs"], opts=qryOpts)
            userInfo = siteInfo["users"][smpConfig["responsible"]]
            ## use the prefixes to figure out the site - fingers crossed
            site_by_prefix = dict()
            for site, prefixes in userInfo["prefix"].items():
                for pref in prefixes:
                    if pref in site_by_prefix:
                        raise KeyError("Prefix {0} is present twice :-(")
                    site_by_prefix[pref] = site
            by_site = defaultdict(list)
            site_prefixes = defaultdict(list)
            anyNotGood = False
            locFiles = []
            for lfn in dsFiles:
                try:
                    pref, site = next((p,s) for p,s in site_by_prefix.items() if lfn.startswith(p))
                    locFiles.append(os.path.join(args.dest, lfn[len(pref):]))
                    by_site[site].append(lfn)
                    if pref not in site_prefixes[site]:
                        site_prefixes[site].append(pref)
                except StopIteration:
                    logger.error("LFN {0} does not start with any of the prefixes {1!s}".format(lfn, list(site_by_prefix.keys())))
                    anyNotGood = True
            if anyNotGood:
                raise RuntimeError("Some LFNs do not start with any of the prefixes")
            assert len(locFiles) == len(dsFiles)
            assert sum(len(siteFiles) for siteFiles in by_site.values()) == len(dsFiles)
            locFName = os.path.join(locfnDir, "{0}.txt".format(smpName))
            if os.path.exists(locFName):
                raise RuntimeError("File {0} already exists!".format(locFName))
            with open(locFName, "w") as locF:
                locF.write("\n".join(locFiles))
            ## also make local file list
            for site, dsSiteFiles in by_site.items():
                uslfndir = os.path.join(lfnDir, "_".join((userInfo["username"], site)))
                createDirIfNeeded(uslfndir)
                uslfnName = os.path.join(uslfndir, smpName)
                if os.path.exists(uslfnName):
                    raise RuntimeError("File {0} already exists!".format(uslfnName))
                with open(uslfnName, "w") as uslfnF:
                    uslfnF.write("\n".join(dsSiteFiles))
                uskey = (userInfo["username"], site)
                usersite_filelists[uskey].append(uslfnName)
                for pref in site_prefixes[site]:
                    if pref not in usersite_prefixes[uskey]:
                        usersite_prefixes[uskey].append(pref)
            logger.debug("Finished writing files for sample {0}".format(smpName))
        ## print the commands
        commands = [ ([
            "../../scripts/sync_srm.py", "-j5",
            "--srm={0}".format(siteInfo["srms"][site]),
            "--dest={0}".format(args.dest),
            "--gfalenv={0}".format(os.path.expanduser("~/clean_gfal_env.json")),
            ]+[ "--lfn-strip={0}".format(pref) for pref in prefixes]
            + usersite_filelists[(user, site)]
            ) for (user, site), prefixes in usersite_prefixes.items() if site != args.homesite ]
        logger.info("Commands to transfer these:\n{0}".format(
            "\n".join(" ".join(cmd) for cmd in commands)))
        cmdFile = os.path.join(args.output, "transfer.sh")
        if os.path.exists(cmdFile):
            raise RuntimeError("File {0} already exists".format(cmdFile))
        with open(cmdFile, "w") as cmdF:
            cmdF.write("\n".join(" ".join(cmd) for cmd in commands))
        if args.doTransfers:
            for cmd in commands:
                logger.info("Now calling {0}".format(" ".join(cmd)))
                subprocess.check_call(cmd)
    else:
        logger.info("No samples selected, done")

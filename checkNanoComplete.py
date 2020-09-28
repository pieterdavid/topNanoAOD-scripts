#!/usr/bin/env python
import subprocess
import logging
logger = logging.getLogger(__name__)
import os.path
from pprint import pformat

def listDatasets(pattern, opts=""):
    res = subprocess.check_output(["dasgoclient", "-query", "dataset dataset={0} {1}".format(pattern, opts)])
    return [ ln.strip() for ln in res.split("\n") if ln.strip() ]

def getParents(dataset, opts=""):
    res = subprocess.check_output(["dasgoclient", "-query", "parent dataset={0} {1}".format(dataset, opts)])
    return [ ln.strip() for ln in res.split("\n") if ln.strip() ]

def listFiles(pattern, opts=""):
    res = subprocess.check_output(["dasgoclient", "-query", "file dataset={0} {1}".format(pattern, opts)])
    return [ ln.strip() for ln in res.split("\n") if ln.strip() ]

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Check the parent of NanoAOD samples, and make sure they've been completely processed")
    parser.add_argument("filelist", nargs="*")
    parser.add_argument("--instance", default="prod/global", help="DBS instance (prod/global, prod/phys03 etc.)")
    parser.add_argument("--from-query", action="append", help="DAS query dataset name pattern (multiple allowed) to get the list of datasets")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--outputLFNs", help="Output directory for the LFN lists")
    parser.add_argument("--outputYAML", help="YAML output file (topNanoAOD-datasets format)")
    parser.add_argument("--recoveryMasks", help="Directory for lumi masks for recovery tasks")
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    qryOpts = ""
    if args.instance != "prod/global":
        qryOpts = " instance={0}".format(args.instance)
    datasets = []
    for fn in args.filelist:
        with open(fn) as lsF:
            datasets += [ ln.strip() for ln in lsF if ln.strip() and not ln.startswith("#") ] ## nonempty, and not starting with a #
    if args.from_query:
        for query in args.from_query:
            try:
                datasets += listDatasets(query, opts=qryOpts)
            except subprocess.CalledProcessError as ex:
                logger.exception("CalledProcessError for query {0}, skipping".format(query))
    nInDS = len(datasets)
    datasets = list(set(datasets))
    if len(datasets) != nInDS:
        logger.info("Removed {0:d} duplicates in input datasets".format(nInDS-len(datasets)))
    logger.info("Checking {0:d} datasets".format(len(datasets)))
    ds_complete, ds_incomplete = [], []
    topDatasetsYaml = dict()
    from CRABClient.UserUtilities import getLumiListInValidFiles
    for ds in datasets:
        parents = getParents(ds, opts=qryOpts)
        logger.debug("P: {0} -> {1}".format(ds, ", ".join(parents)))
        ll_nano = getLumiListInValidFiles(ds)
        ll_mini_items = { p : getLumiListInValidFiles(p, "global") for p in parents }
        if len(ll_mini_items) == 1:
            ll_mini = ll_mini_items.values()[0]
        elif len(ll_mini_items) > 1:
            items = list(ll_mini_items.values())
            ll_mini = items[0]
            for itm in items[1:]:
                ll_mini += itm
        else:
            raise RuntimeError("No parents for dataset {0}".format(ds))
        ll_remain = ll_mini - ll_nano
        comment = None
        if len(ll_remain) != 0:
            logger.error("Dataset {0} is missing luminosity blocks for {1:d} runs".format(ds, len(ll_remain)))
            comment = "Not completely processed yet"
            ds_incomplete.append(ds)
        else:
            ds_complete.append(ds)
        if args.recoveryMasks:
            for parent, ll_mini in ll_mini_items.items():
                ll_remain = ll_mini - ll_nano
                if len(ll_remain) != 0:
                    logger.debug("Missing luminosity blocks for {0:d} runs of {1}".format(len(ll_remain), parent))
                    ll_remain.writeJSON(os.path.join(args.recoveryMasks, "{0}.json".format(parent.replace("/", "__"))))
        ds_prim, ds_sec, ds_type = ds.strip("/").split("/")
        ymlEntry = {
            "dbs": ds,
            "parents": parents,
            }
        if comment:
            ymlEntry["comment"] = comment
        ymlEntry["responsible"] = ds_sec.split("-")[0]
        if "Run201" in ds_sec:
            rn = "Run201{0}".format(ds_sec.split("-")[-2].split("Run201")[1])
            name = "_".join((ds_prim, rn))
        else: ## MC: just process name
            name = ds_prim
        while name in topDatasetsYaml:
            name += "_"
        topDatasetsYaml[name] = ymlEntry
        logger.debug("Entry for {0}: {1}".format(name, pformat(ymlEntry)))
    logger.info("Complete datasets: {0:d}/{1:d}\n{2}".format(len(ds_complete), len(datasets), "\n".join(ds_complete)))
    logger.info("Datasets with missing luminosity blocks: {0:d}/{1:d}\n{2}".format(len(ds_incomplete), len(datasets), "\n".join(ds_incomplete)))
    if args.outputLFNs:
        outdir = args.outputLFNs
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        if not os.path.isdir(outdir):
            logger.error("{0} is not a directory!".format(outdir))
        else:
            for ds in ds_complete:
                dsFiles = listFiles(ds, opts=qryOpts)
                if ds.endswith("SIM"):
                    fn = ds.strip("/").split("/")[0]
                else:
                    fn = "{0}.txt".format(ds.strip("/").replace("/", "__"))
                fn = os.path.join(outdir, fn)
                if os.path.exists(fn):
                    logger.error("File {0} exists, skipping".format(fn))
                else:
                    with open(fn, "w") as lfnF:
                        lfnF.write("\n".join(dsFiles))
    if args.outputYAML:
        import yaml
        with open(args.outputYAML, "w") as ymlF:
            yaml.dump(topDatasetsYaml, ymlF)

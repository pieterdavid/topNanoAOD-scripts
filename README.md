# A few helper scripts for working with TopNanoAOD (or other privately produced) datasets

For more information please ask in the [TopNanoAOD mattermost channel](https://mattermost.web.cern.ch/cms-exp/channels/topnanoaod).

- `checkNanoComplete.py` (to be run in a python2.7 CRAB environment) can be used to check
  if NanoAOD datasets cover the same runs and luminosity blocks as the parent MiniAOD datasets.
  It takes either a file with DAS paths, or a query with wildcards (through `--from-query`);
  the DBS instance can be configured (for the parent dataset the global one is always used).
  Other than the printout, it can produce text files with LFNs, a YAML file fragment
  in [topNanoAOD-datasets](https://github.com/cms-top/topNanoAOD-datasets)-like format (incomplete),
  and XML masks with the missing runs and luminosity blocks to submit
  a [recovery task](https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#Recovery_task_How).
- `sync_srm.py` (recent python3 for asyncio): copy datasets between sites with SRM
  (recursively scanning directories, or from a list of LFNs), in parallel;
  since `gfal-copy` is a python2 tool, a JSON file with the environment to run this command in can be passed.
- `lfnAndFileListsFromTopNanoDatasets.py` (python3, same as `sync_srm.py`):
  automate dataset copies between sites using [topNanoAOD-datasets](https://github.com/cms-top/topNanoAOD-datasets)
  and a file that lists the selected processes.
  Information about the input paths, SRM servers etc. should be supplied in a YAML file
  (not included here - please ask, or infer the format from the code).
  The first step is fairly straightforward: DAS queries per site or stageout area
  (the outputs of this are saved); the second (optional, off by default) step uses `sync_srm.py` to do the transfers
  (and prints the commands, such that they can be resumed when stuck; single quotes should be added around the SRM argument then).

All of these also require a valid grid proxy.

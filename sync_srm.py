#!/usr/bin/env python3
"""Synchronize files using srm, with asyncio"""
__author__ = "Pieter David <pieter.david@uclouvain.be>"
__date__ = "25 June 2019"

import asyncio
import fnmatch
from functools import partial
from itertools import chain, count
import logging
logger = logging.getLogger()
import os, os.path
import subprocess

LS_COMMAND = "srmls"
LS_L1_PREFIX = " "*6
DOWNLOAD_COMMAND = "gfal-copy"
gfalenv = None

def formatFileSize(num, suffix="B"):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def joinUrl(*parts):
    """Join URL, collapsing the slashes in between different parts to one"""
    if len(parts) >= 2:
        parts = [ p for i,p in zip(count(), parts) if i == 0 or p.strip("/") != "." ]
    if len(parts) < 1:
        raise ValueError("Need at least one argument")
    elif len(parts) == 1:
        return parts[0]
    else:
        joined = "/".join(chain([parts[0].rstrip("/")], ( p.strip("/") for p in parts[1:-1] ), [parts[-1].lstrip("/")]))
        return "/".join(joined.split("/./"))

async def subproc_check_call(*args, timeout=None, env=None):
    """asyncio version of subprocess.check_call"""
    proc = await asyncio.create_subprocess_exec(*args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
    _stdout, _stderr = await proc.communicate()
    if proc.returncode:
        logger.error("Command '{0}' exited with status code {1:d}".format(" ".join(args), proc.returncode))

async def subproc_check_output(*args, timeout=None, env=None):
    """asyncio version of subprocess.check_output"""
    proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
    stdout, stderr = await proc.communicate()
    if proc.returncode:
        logger.error("Command '{0}' exited with status code {1:d}\n{2}".format(" ".join(args), proc.returncode, stderr))
    return stdout

async def list_directory(srm, path, semaphore):
    """List directory contents with SRM

    :param srm: first part of the url (should contain everything that's not in the srmls entry output, i.e. the SRM server part)
    :param path: second part of the url (should be long enough to be unique in each path, entryLine.split(path)[1] is supposed to be the last part)

    :returns: (list of subdirectories, list of files)
    """
    fullPath = joinUrl(srm, path)
    pref = "{0}/".format(path.rstrip("/")) ## make sure we have exactly one trailing slash
    async with semaphore:
        output = await subproc_check_output(LS_COMMAND, fullPath)
    entries = [ (int(ln.split()[0]), ln.split()[1].split(pref)[1]) for ln in output.decode().strip().split("\n") if ln.startswith(LS_L1_PREFIX) ]
    return [ ln for sz,ln in entries if ln.endswith("/") ], [ (sz,ln) for sz,ln in entries if not ln.endswith("/") ]

def parse_args(args=None):
    from argparse import ArgumentParser
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--srm", help="SRM server with storage root (use lcg-infosites to get a list)")
    parser.add_argument("--lfn-strip", default=[], action="append", help="Leading part of the LFN to remove (and replace by --dest/-o)")
    parser.add_argument("--dest", "-o", default=".", help="Destination (current directory by default)")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Dry run: print the list of files that would be downloaded")
    parser.add_argument("--filter", default="*.root", help="Filter for filenames ('*.root' by default)")
    parser.add_argument("--dirfilter", default=[], action="append", help="Filter for the crab task name part of the path")
    parser.add_argument("--max-depth", type=int, default=1, help="Maximum depth to scan (1 by default)")
    parser.add_argument("--gfalenv", help="JSON file with environment variables to call gfal-copy")
    parser.add_argument("-j", "--nProcesses", type=int, default=1, help="Number of parallel processes to use for downloading (1 by default)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose printout mode")
    parser.add_argument("path", nargs="+", help="Path in the store (starting from the --srm path) to synchronize")
    return parser.parse_args(args=args)

class DownloadTask(object):
    """Object representation of a download task (to help running them in parallel)"""
    def __init__(self, origUrl, dest, nBytes=None):
        self.origUrl = origUrl
        self.dest = dest
        self.nBytes = nBytes
        self._done = self._checkDone()
    def _makeDirIfNeeded(self):
        dirName = os.path.dirname(self.dest)
        if not os.path.isdir(dirName):
            os.makedirs(dirName)
    def _checkDone(self):
        if not os.path.exists(self.dest):
            return False
        else:
            dskSize = os.path.getsize(self.dest)
            if dskSize >= self.nBytes:
                return True
            else:
                logger.warning("Disk size of {0} is {1:d}, but {2:d} is expected from SRM, removing".format(self.dest, dskSize, self.nBytes))
                os.remove(self.dest)
                return False
    def _dlArgs(self):
        return [DOWNLOAD_COMMAND, self.origUrl, os.path.abspath(self.dest)]
    async def run(self, semaphore):
        if self._done:
            return self, "OK", "Already downloaded"
        if self._done is None:
            return self, "WARNING", "File {0} exists, skipping".format(self.dest)
        else:
            self._makeDirIfNeeded()
            #print("DEBUG: Calling {0}".format(" ".join(self._dlArgs())))
            async with semaphore:
                await subproc_check_call(*self._dlArgs(), env=gfalenv)
            self._done = True
            return self, "OK", "Downloaded"
    def __str__(self):
        return "% {0} ({1}, {2})".format(" ".join(self._dlArgs()), formatFileSize(self.nBytes), "DONE" if self._done else "TODO")

async def agen_to_list(agen):
    return [ it async for it in agen ]

async def harvestDownloadTasks(srm, origBase, path, currentLevel=0, remainingLevels=0, destBase=".", dirSel=(lambda iLv,dirName : True), fnSel=(lambda fn : True), semaphore=None):
    logger.debug("harvestDownloadTasks with srm={srm}, origBase={origBase}, path={path}, currentLevel={currentLevel:d}, remainingLevels={remainingLevels:d}, destBase={destBase}".format(
                    srm=srm, origBase=origBase, path=path, currentLevel=currentLevel, remainingLevels=remainingLevels, destBase=destBase))
    subdirs, files = await list_directory(srm, joinUrl(origBase, path), semaphore=semaphore)
    for fileBytes,filePath in files:
        if fnSel(filePath):
            yield DownloadTask(joinUrl(srm, origBase, path, filePath), joinUrl(destBase, path, filePath), nBytes=fileBytes)
    if remainingLevels > 0:
        for fut in asyncio.as_completed(list(agen_to_list(
                harvestDownloadTasks(srm, origBase, joinUrl(path, subdir),
                    currentLevel=currentLevel+1, remainingLevels=(remainingLevels-1), destBase=destBase,
                    dirSel=dirSel, fnSel=fnSel, semaphore=semaphore)) for subdir in subdirs if dirSel(currentLevel, subdir))):
            for it in await fut:
                yield it

async def downloadTasksForPath(srm, origBase, path, remainingLevels=0, destBase=".", dirSel=(lambda iLv,dirName : True), fnSel=(lambda fn : True), semaphore=None):
    ptasks = [ t async for t in harvestDownloadTasks(srm, origBase, path, remainingLevels=remainingLevels, destBase=destBase, dirSel=dirSel, fnSel=fnSel, semaphore=semaphore) ]
    if len(ptasks) > 0:
        logger.info("List of files to synchronize for {0} ({1:d} files, {2})".format(joinUrl(srm, origBase, path), len(ptasks), formatFileSize(sum(t.nBytes for t in ptasks))))
        for t in ptasks:
            logger.debug(str(t))
        if not args.dry_run:
            for bd in set(os.path.dirname(t.dest) for t in ptasks):
                if not os.path.isdir(bd):
                    os.makedirs(bd)
    return ptasks

async def downloadTasksForLFNs(srm, lfnList, dest, lfnStrip=None):
    from collections import defaultdict
    lfn_by_dir = defaultdict(list)
    for lfn in lfnList:
        i = lfn.rfind("/")
        ldir = lfn[:i]
        lbase = lfn[(i+1):]
        lfn_by_dir[ldir].append(lbase)
    tasks = []
    for ldir, fnames in lfn_by_dir.items():
        dirlsl = await subproc_check_output("gfal-ls", "-l", joinUrl(srm, ldir), env=gfalenv)
        bytes_per_entry = dict((ln.strip().split()[8], int(ln.strip().split()[4])) for ln in dirlsl.decode().strip().split("\n") if ln.strip())
        if lfnStrip:
            prefix = next(pref for pref in lfnStrip if ldir.startswith(pref))
            destDir = os.path.join(dest, ldir[len(prefix):].lstrip("/"))
        else:
            destDir = os.path.join(dest, ldir.lstrip("/"))
        if ( not args.dry_run ) and ( not os.path.isdir(destDir) ):
            os.makedirs(destDir)
        for fnm in fnames:
            tasks.append(DownloadTask(joinUrl(srm, ldir, fnm), joinUrl(destDir, fnm), nBytes=bytes_per_entry[fnm]))
    return tasks

async def main(args):
    fnFilter = partial((lambda pat, fn : fnmatch.fnmatchcase(fn, pat) and fn != "muonSet_randomized.root"), args.filter) ## FIXME make this also configurable
    dirFilter = lambda iLv, dirName : True
    if args.dirfilter:
        dirFilter = partial((lambda pats, iLv, dirName : iLv != 1 or any(fnmatch.fnmatchcase(os.path.basename(dirName.rstrip("/")), pat) for pat in pats)), args.dirfilter)

    lsReqSemaphore = asyncio.Semaphore(10) ## TODO put 10 or so
    tasks = []
    for path in args.path:
        ptasks = None
        if os.path.isfile(path): ## LFN list
            logger.debug("Reading LFNs from {0}".format(path))
            with open(path) as lfnLF:
                lfns = [ ln.strip() for ln in lfnLF if ln.strip() ]
                ptasks = await downloadTasksForLFNs(args.srm, lfns, args.dest, lfnStrip=args.lfn_strip)
        else: ## recursive
            logger.debug("{0} is not a file, going recursive".format(path))
            ptasks = await downloadTasksForPath(args.srm, path, ".", remainingLevels=(args.max_depth-1), destBase=args.dest, dirSel=dirFilter, fnSel=fnFilter, semaphore=lsReqSemaphore)
        if ptasks:
            tasks += ptasks
            logger.debug("Still to download for {0}: {1:d} files, {2}".format(path, sum(1 for t in ptasks if not t._done), formatFileSize(sum(t.nBytes for t in ptasks if not t._done))))
    logger.info("Still to download in total: {0:d} files, {1}".format(sum(1 for t in tasks if not t._done), formatFileSize(sum(t.nBytes for t in tasks if not t._done))))
    tasks = [ t for t in tasks if not t._done ]
    if tasks:
        logger.debug("An example task: {0!s}".format(tasks[0]))
    if len(tasks) > 0 and not args.dry_run:
        logger.info("Launching {0:d} simultaneous downloads".format(args.nProcesses))
        downloadSemaphore = asyncio.Semaphore(args.nProcesses)
        countOK, countDone = 0, 0
        _progThresholds = iter([ int(round(x*len(tasks)/100)) for x in range(1,101) ])
        nextProg = next(_progThresholds)
        for fut in asyncio.as_completed(list(t.run(downloadSemaphore) for t in tasks)):
            tsk,stat,msg = await fut
            countDone += 1
            if stat != "OK":
                strm = logger.error if stat == "ERROR" else logger.warning
                strm("{0} while downloading {1}".format(msg, tsk.origUrl))
            else:
                countOK += 1
                logger.debug("Downloaded {0}".format(tsk.origUrl))
            if countDone == nextProg:
                logger.info("Finished {0:d}/{1:d} downloads ({2:d} successful)".format(countDone, len(tasks), countOK))
                try:
                    nextProg = next(_progThresholds)
                except StopIteration:
                    pass
        logger.info("{0:d}/{1:d} downloads finished successfully".format(countOK, len(tasks)))

if __name__ == "__main__":
    args = parse_args()
    if args.gfalenv is not None:
        import json
        with open(args.gfalenv) as ef:
            gfalenv = json.load(ef)

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))
    ## TODO change to asyncio.run(main(args)) with python 3.7
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
    loop.close()

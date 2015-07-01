#!/bin/env python

###
#
# This tool will replicate the file and directory structure as it is in remotePath
# to destinationPath. Only directories that contain filesToCopy will be copied, others will
# be ignored.
#
###

import thread
import subprocess
import os.path

debugMode=True

destinationBase="file://"
destinationPath="/tmp/thaarres/"

remoteSrmBase="srm://t3se01.psi.ch:8443/srm/managerv2?SFN="
remotePath="/pnfs/psi.ch/cms/trivcat/store/t3groups/uniz-higgs/Spring15/TTJets_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/"
# remotePath="/pnfs/psi.ch/cms/trivcat/store/t3groups/uniz-higgs/Spring15/QCD_PtBinned_TuneCUETP8M1_13TeV_pythia8/QCD_PT_1000to1400/"
# remotePath="/pnfs/psi.ch/cms/trivcat/store/t3groups/uniz-higgs/Spring15/GluGluToRadionToHHTo4B_M_900_narrow/"
filesToCopy=".root"

srmlsCount=900
srmlsOffset = 0
listCommand="gfal-ls"
# listCommand += " -offset %i"
listCommandSuffix=" "
copyCommand="gfal-copy"

def getDirList(currentDir, srmlsOffset):

  dirList = []
  thisListCommand = listCommand
  print "thisListCommand %s "  %(thisListCommand)
  queryString="%s '%s%s' %s" %(thisListCommand, remoteSrmBase, currentDir, listCommandSuffix)
  if debugMode: print queryString
  
  print "Debugging in getDirList**********************"
  dirListFile = open("dirlist.tmp", "w")
  lock=thread.allocate_lock()
  lock.acquire()
  listProcess=subprocess.Popen(queryString, stdout=dirListFile, stderr=subprocess.PIPE, shell=True)
  listProcess.wait()
  lock.release()
  dirListFile.close()
  dirListFile = open("dirlist.tmp", "r")
  firstLine = True
  for dirContent in dirListFile.read().splitlines():
    if (queryString.find("srmls") >= 0) and (firstLine): # first line is current directory
      firstLine = False
      continue
    if not (dirContent.find("ExoDibosonRes") >= 0):
      if debugMode: print dirContent
      dirList.append(dirContent)
  dirList = filter(None, dirList) #filter to also get rid of empty entries
  dirListFile.close()
   
   
  if len(dirList) == 0: print "Returning empty list"
  else : print "Returning list of size: %i - Last entry: %s" %(len(dirList), dirList[len(dirList)-1])
  return dirList


def copyFiles(currentFile, forceOverwrite = False):
  
  # could check first that file does not already exist
  psiOutPath = "%s%s" %(destinationPath, currentFile.replace(remotePath,""))
  if (os.path.isfile("%s"%psiOutPath) ):
    print "Skipping %s - already exists" %psiOutPath
    return
  copyString="%s '%s%s%s' '%s%s'" %(copyCommand, remoteSrmBase,remotePath, currentFile, destinationBase, psiOutPath)
  if debugMode: print "Copy string: %s" %copyString
  lock=thread.allocate_lock()
  lock.acquire()
  copyProcess=subprocess.Popen(copyString, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  copyProcess.wait()
  lock.release()
  errors = copyProcess.stderr.read()
  if ( len(errors) > 0):
    print "WARNING, there has been an error!"
    print errors 
  dirList = copyProcess.stdout.read()


def loopOrCopy(thisDir):
  if (thisDir.find(filesToCopy) >= 0):
    copyFiles(thisDir)
  else:
    print "Getting next dir list for %s" %thisDir
    nextDirList = getDirList(thisDir , srmlsOffset)
    if debugMode: print nextDirList
    for dirContent in nextDirList:
       if len(dirContent) == 0: continue
       loopOrCopy(dirContent)
      


def main():

#   print "Getting initial directory"
#   thisDirList = getDirList(remotePath)
#   if debugMode: print thisDirList
#   print "Starting loop"
#   for dirContent in thisDirList:
#     loopOrCopy(dirContent)
# 
#   print "%i" %len(thisDirList)
#   if (len(thisDirList) >= srmlsCount-1) and (listCommand.find("srmls")):
#     srmlsOffset = srmlsCount #or +1?
#     while (len(thisDirList) > 0):
#       thisDirList = getDirList(remotePath, srmlsOffset)
#       srmlsOffset += srmlsCount
#       for dirContent in thisDirList:
#         loopOrCopy(dirContent)
  loopOrCopy(remotePath)
    
  print "Done."


if __name__ == "__main__":
  main()

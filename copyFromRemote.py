#!/bin/env python

###
#
# This tool will replicate the file and directory structure as it is in remotePath
# to psiPath. Only directories that contain filesToCopy will be copied, others will
# be ignored.
#
###

import thread
import subprocess
import os.path

debugMode=True

psiSrmBase="srm://t3se01.psi.ch:8443/srm/managerv2?SFN="
psiPath="/pnfs/psi.ch/cms/trivcat/store/t3groups/uniz-higgs/pattuple/mc/aniello/AbelianZprime_hh_TauTauBB_LHC8_M1200"

remoteSrmBase="srm://gridse2.pg.infn.it:8444/srm/managerv2?SFN="
remotePath="/cms/store/user/aspiezia/AbelianZprime_hh_TauTauBB_LHC8_M1200"

filesToCopy=".root"

srmlsCount=900
srmlsOffset =0
listCommand="srmls -count %i" %srmlsCount
listCommand += " -offset %i"
listCommandSuffix="2>&1 | awk '{ print $2; }'"
copyCommand="lcg-cp -b -D srmv2"


def getDirList(currentDir, srmlsOffset):

  dirList = []
  thisListCommand = listCommand %srmlsOffset
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
  print "len(dirList) %i , srmlsCount %i , offset %i "  %(len(dirList),srmlsCount,srmlsOffset)
  # call function again with offset in case there are more than srmlsCount entries
  if (len(dirList) >= srmlsCount -1 ):
    print "%s is a large dir, looping further" %currentDir
    print "Size: %i - Last entry: %s" %(len(dirList), dirList[len(dirList)-1])
    srmlsOffset += srmlsCount
    print "len(dirList) %i , srmlsCount %i , offset %i "  %(len(dirList),srmlsCount,srmlsOffset)
    dirList += getDirList(currentDir, srmlsOffset)
   
   
  if len(dirList) == 0: print "Returning empty list"
  else : print "Returning list of size: %i - Last entry: %s" %(len(dirList), dirList[len(dirList)-1])
  return dirList


def copyFiles(currentFile, forceOverwrite = False):
  
  # could check first that file does not already exist
  psiOutPath = "%s%s" %(psiPath, currentFile.replace(remotePath,""))
  if (os.path.isfile("%s"%psiOutPath) ):
    print "Skipping %s - already exists" %psiOutPath
    return
  copyString="%s '%s%s' '%s%s'" %(copyCommand, remoteSrmBase, currentFile, psiSrmBase, psiOutPath)
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

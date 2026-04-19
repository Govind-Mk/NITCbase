#include "OpenRelTable.h"
#include<stdlib.h>
#include <cstring>
#include <stdio.h>
OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
OpenRelTable::OpenRelTable() {

  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free=true;
  }

  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

   RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;
  relCacheEntry.searchIndex.block = -1;
  relCacheEntry.searchIndex.slot  = -1;

  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  relCatBlock.getRecord(attrCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

  struct RelCacheEntry attrCacheEntry;
  RelCacheTable::recordToRelCatEntry(attrCatRecord, &attrCacheEntry.relCatEntry);
  attrCacheEntry.recId.block = RELCAT_BLOCK;
  attrCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  attrCacheEntry.searchIndex.block = -1;
  attrCacheEntry.searchIndex.slot  = -1;
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = attrCacheEntry;

  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  AttrCacheEntry *tail=nullptr,*head=nullptr;
  for(int i=0;i<RELCAT_NO_ATTRS;i++)
  {
    attrCatBlock.getRecord(attrCatRecord,i);
    AttrCacheEntry* attrCacheEntry =(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block=ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot=i;
    attrCacheEntry->next = nullptr;
    if(head!=nullptr)
    {
        tail->next=attrCacheEntry;
    }
    else
    {
        head=attrCacheEntry;
    }
    tail=attrCacheEntry;
  }
if(tail != nullptr)
    tail->next = nullptr;

  AttrCacheTable::attrCache[RELCAT_RELID] = head;

   head=nullptr;
  tail=nullptr;
  for(int i=RELCAT_NO_ATTRS;i<RELCAT_NO_ATTRS+ATTRCAT_NO_ATTRS;i++)
  {
    attrCatBlock.getRecord(attrCatRecord,i);
    AttrCacheEntry* attrCacheEntry =(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block=ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot=i;
    attrCacheEntry->next = nullptr; 
    if(head!=nullptr)
    {
        tail->next=attrCacheEntry;
    }
    else
    {
        head=attrCacheEntry;
    }
    tail=attrCacheEntry;
  }
 if(tail != nullptr)
    tail->next = nullptr;
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
  tableMetaInfo[RELCAT_RELID].free=false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName,"RELATIONCAT");
  tableMetaInfo[ATTRCAT_RELID].free=false;
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName,"ATTRIBUTECAT");
}

OpenRelTable::~OpenRelTable() {
   for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i);
    }
  }
  if(RelCacheTable::relCache[ATTRCAT_RELID]->dirty)
  {
    Attribute record[ATTRCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry,record);
    RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;
    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(record,recId.slot);
  }
   if(RelCacheTable::relCache[RELCAT_RELID]->dirty)
   {
      Attribute record[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[RELCAT_RELID]->relCatEntry,record);
    RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;
    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(record,recId.slot);
   }
  for (int i = 0; i <2; i++) {
    if(RelCacheTable::relCache[i] != nullptr)
    {
        free(RelCacheTable::relCache[i]);
        RelCacheTable::relCache[i] = nullptr;
    }
  }
  for (int i = 0; i <2; i++) {
    if(AttrCacheTable::attrCache[i] != nullptr)
    {
        AttrCacheEntry *curr=AttrCacheTable::attrCache[i];
        while(curr!=nullptr)
        {
            AttrCacheEntry *temp=curr;
            curr=curr->next;
            free(temp);
        }
        AttrCacheTable::attrCache[i] = nullptr;
    }
  }
  
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
    for(int i=0;i<MAX_OPEN;i++)
  {
    if((!tableMetaInfo[i].free) && (!strcmp(tableMetaInfo[i].relName,relName)))
    return i;
  }
  return E_RELNOTOPEN;
}
  
int OpenRelTable::getFreeOpenRelTableEntry() {
  for(int i=2;i<MAX_OPEN;i++)
  {
    if(tableMetaInfo[i].free)
    return i;
  }
  return E_CACHEFULL;
}

int OpenRelTable::openRel( char relName[ATTR_SIZE]) {
  int relId=getRelId(relName);
  if(relId!=E_RELNOTOPEN){
         return relId;
  }

     relId=getFreeOpenRelTableEntry();
  if (relId==E_CACHEFULL){
    return E_CACHEFULL;
  }

 Attribute attrVal;
 strcpy(attrVal.sVal,relName);
char name[ATTR_SIZE];
strcpy(name, RELCAT_ATTR_RELNAME);
 RelCacheTable::resetSearchIndex(RELCAT_RELID);

  RecId relcatRecId= BlockAccess::linearSearch(RELCAT_RELID,name,attrVal,EQ);

  if (relcatRecId.block==-1 && relcatRecId.slot==-1) {
    return E_RELNOTEXIST;
  }
RecBuffer relCatBlock(relcatRecId.block);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord,relcatRecId.slot);

   RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = relcatRecId.block;
  relCacheEntry.recId.slot = relcatRecId.slot;
  relCacheEntry.searchIndex.block = -1;
  relCacheEntry.searchIndex.slot  = -1;
 
  RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[relId]) = relCacheEntry;

  AttrCacheEntry* listHead=nullptr;
  AttrCacheEntry* tail=nullptr;

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  
  while(1)
  {   
      RecId attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,name,attrVal,EQ);
      if(attrcatRecId.block==-1 && attrcatRecId.slot==-1)
      break;
      RecBuffer attrCatBlock(attrcatRecId.block);
    attrCatBlock.getRecord(attrCatRecord,attrcatRecId.slot);
    AttrCacheEntry* attrCacheEntry =(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block=attrcatRecId.block;
    attrCacheEntry->recId.slot=attrcatRecId.slot;
    attrCacheEntry->next = nullptr;
    if(listHead!=nullptr)
    {
        tail->next=attrCacheEntry;
    }
    else
    {
        listHead=attrCacheEntry;
    }
    tail=attrCacheEntry;

  
  }
if(tail != nullptr)
    tail->next = nullptr;
AttrCacheTable::attrCache[relId] = listHead;
tableMetaInfo[relId].free=false;
strcpy(tableMetaInfo[relId].relName,relName);
  return relId;
}

int OpenRelTable::closeRel(int relId) {
	if (relId == ATTRCAT_RELID || relId == RELCAT_RELID) {
		return E_NOTPERMITTED;
	}

	if (relId >= MAX_OPEN || relId < 0) {
		return E_OUTOFBOUND;
	}

	if (tableMetaInfo[relId].free) {
		return E_RELNOTOPEN;
	}

	if (RelCacheTable::relCache[relId]->dirty)
	{
		Attribute record[RELCAT_NO_ATTRS];
		RelCatEntry relCatEntry;
		RelCacheTable::getRelCatEntry(relId, &relCatEntry);
		RelCacheTable::relCatEntryToRecord(&relCatEntry, record);

		RecId recId = RelCacheTable::relCache[relId]->recId;
		RecBuffer relCatBlock(recId.block);

		relCatBlock.setRecord(record, recId.slot);
	}
	free(RelCacheTable::relCache[relId]);

	AttrCacheEntry* entry = AttrCacheTable::attrCache[relId], * temp = nullptr;
	while (entry) {
		if (entry->dirty) {
			Attribute rec[ATTRCAT_NO_ATTRS];
			AttrCacheTable::attrCatEntryToRecord(&entry->attrCatEntry, rec);
			
			RecBuffer attrCatBlk(entry->recId.block);
			attrCatBlk.setRecord(rec, entry->recId.slot);
        }

		temp = entry;
		entry = entry->next;
		free(temp);
	}

	RelCacheTable::relCache[relId] = nullptr;
	AttrCacheTable::attrCache[relId] = nullptr;

	tableMetaInfo[relId].free = true;

	return SUCCESS;
}
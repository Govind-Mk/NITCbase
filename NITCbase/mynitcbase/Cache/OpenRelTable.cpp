#include "OpenRelTable.h"
#include<stdlib.h>
#include <cstring>
#include <stdio.h>
OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free=true;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

   RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;
  relCacheEntry.searchIndex.block = -1;
  relCacheEntry.searchIndex.slot  = -1;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;


  
  /**** setting up Attribute Catalog relation in the Relation Cache Table ****/

  // set up the relation cache entry for the attribute catalog similarly
  // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT
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

  // set the value at RelCacheTable::relCache[ATTRCAT_RELID]


  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  //Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  // iterate through all the attributes of the relation catalog and create a linked
  // list of AttrCacheEntry (slots 0 to 5)
  // for each of the entries, set
  //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
  //    attrCacheEntry.recId.slot = i   (0 to 5)
  //    and attrCacheEntry.next appropriately
  // NOTE: allocate each entry dynamically using malloc
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

  // set the next field in the last entry to nullptr

  AttrCacheTable::attrCache[RELCAT_RELID] = head;



  /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

  // set up the attributes of the attribute cache similarly.
  // read slots 6-11 from attrCatBlock and initialise recId appropriately
  
  // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
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
  // free all the memory that you allocated in the constructor
   for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i); // we will implement this function later
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
/* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/
    for(int i=0;i<MAX_OPEN;i++)
  {
    if((!tableMetaInfo[i].free) && (!strcmp(tableMetaInfo[i].relName,relName)))
    return i;
  }
  return E_RELNOTOPEN;
  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.
}
  
int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
  for(int i=2;i<MAX_OPEN;i++)
  {
    if(tableMetaInfo[i].free)
    return i;
  }
  return E_CACHEFULL;
  // if found return the relation id, else return E_CACHEFULL.
}

int OpenRelTable::openRel( char relName[ATTR_SIZE]) {
  int relId=getRelId(relName);
  if(relId!=E_RELNOTOPEN){
    // (checked using OpenRelTable::getRelId())
         return relId;
    // return that relation id;
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
     relId=getFreeOpenRelTableEntry();
  if (relId==E_CACHEFULL){
    return E_CACHEFULL;
  }

  // let relId be used to store the free slot.

  /****** Setting up Relation Cache entry for the relation ******/

  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/
 Attribute attrVal;
 strcpy(attrVal.sVal,relName);
char name[ATTR_SIZE];
strcpy(name, RELCAT_ATTR_RELNAME);
 RelCacheTable::resetSearchIndex(RELCAT_RELID);
  // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
  RecId relcatRecId= BlockAccess::linearSearch(RELCAT_RELID,name,attrVal,EQ);

  if (relcatRecId.block==-1 && relcatRecId.slot==-1) {
    // (the relation is not found in the Relation Catalog.)
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
  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[relId]) = relCacheEntry;
  /* read the record entry corresponding to relcatRecId and create a relCacheEntry
      on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
      update the recId field of this Relation Cache entry to relcatRecId.
      use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
    NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
  */

  /****** Setting up Attribute Cache entry for the relation ******/

  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry* listHead=nullptr;
  AttrCacheEntry* tail=nullptr;

  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
      /* read the record entry corresponding to attrcatRecId and create an
      Attribute Cache entry on it using RecBuffer::getRecord() and
      AttrCacheTable::recordToAttrCatEntry().
      update the recId field of this Attribute Cache entry to attrcatRecId.
      add the Attribute Cache entry to the linked list of listHead .*/
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  
  while(1)
  {
      /* let attrcatRecId store a valid record id an entry of the relation, relName,
      in the Attribute Catalog.*/
      
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
  // set the relIdth entry of the AttrCacheTable to listHead.

  /****** Setting up metadata in the Open Relation Table for the relation******/
tableMetaInfo[relId].free=false;
strcpy(tableMetaInfo[relId].relName,relName);
  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.
  return relId;
}

int OpenRelTable::closeRel(int relId) {
  if (relId==RELCAT_RELID || relId==ATTRCAT_RELID)
    return E_NOTPERMITTED;

  if (relId<0 || relId>=MAX_OPEN)
    return E_OUTOFBOUND;

  if (tableMetaInfo[relId].free)
    return E_RELNOTOPEN;

  // free relation cache
  if(RelCacheTable::relCache[relId])
  {
    Attribute record[RELCAT_NO_ATTRS];
    if(RelCacheTable::relCache[relId]->dirty)
    {
      RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[relId]->relCatEntry,record);
      RecId recId=RelCacheTable::relCache[relId]->recId;
      RecBuffer relCatBlock(recId.block);
      relCatBlock.setRecord(record,recId.slot);
    }
    free(RelCacheTable::relCache[relId]);
  }

  // free attribute cache list
  AttrCacheEntry* curr = AttrCacheTable::attrCache[relId];
  while(curr)
  {
    AttrCacheEntry* temp = curr;
    curr = curr->next;
    free(temp);
  }

  RelCacheTable::relCache[relId] = nullptr;
  AttrCacheTable::attrCache[relId] = nullptr;

  tableMetaInfo[relId].free = true;

  return SUCCESS;
}
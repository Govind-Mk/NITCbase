#include "BlockAccess.h"
#include<stdio.h>
#include <cstring>

extern int comparisionCount;

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);

    int block,slot;
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId,&relCatEntry);
        block=relCatEntry.firstBlk;
        slot=0;
    }
    else
    {
        block=prevRecId.block;
        slot=prevRecId.slot+1;
    }

    AttrCatEntry attrBuf;
    AttrCacheTable::getAttrCatEntry(relId,attrName,&attrBuf);
    while (block != -1)
    {
        RecBuffer recBuf(block);
        HeadInfo head;
        recBuf.getHeader(&head);
        
        unsigned char slotMap[head.numSlots];
        recBuf.getSlotMap(slotMap);
        if(slot>=head.numSlots)
        {
            block=head.rblock;
            slot=0;
            continue;
        }
        if(slotMap[slot]==SLOT_UNOCCUPIED)
        {
            slot++;
            continue;
        }
        
        union Attribute rec[head.numAttrs];
        recBuf.getRecord(rec,slot);
        
        int cmpVal;
        cmpVal=compareAttrs(rec[attrBuf.offset],attrVal,attrBuf.attrType);
        comparisionCount++;

        if (
            (op == NE && cmpVal != 0) ||
            (op == LT && cmpVal < 0) || 
            (op == LE && cmpVal <= 0) || 
            (op == EQ && cmpVal == 0) ||
            (op == GT && cmpVal > 0) || 
            (op == GE && cmpVal >= 0)
        ) {
            RecId setIndex{block,slot};
            RelCacheTable::setSearchIndex(relId,&setIndex);
            return RecId{block, slot};
        }

        slot++;
    }

    RelCacheTable::resetSearchIndex(relId);
    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute newRelationName;
    strcpy(newRelationName.sVal,newName);

    RecId searchRec;
    char name[16]=RELCAT_ATTR_RELNAME;
    searchRec=linearSearch(RELCAT_RELID,name,newRelationName,EQ);
    if(searchRec.block!=-1 && searchRec.slot!=-1)
    {
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
    strcpy(oldRelationName.sVal,oldName);
    searchRec=linearSearch(RELCAT_RELID,name,oldRelationName,EQ);
    if(searchRec.block==-1 && searchRec.slot==-1)
    {
        return E_RELNOTEXIST;
    }

   RecBuffer recbuf(searchRec.block);
   
   Attribute rec[RELCAT_NO_ATTRS];
   recbuf.getRecord(rec,searchRec.slot);
    strcpy(rec[RELCAT_REL_NAME_INDEX].sVal,newName);
    recbuf.setRecord(rec,searchRec.slot);
   
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    Attribute attrRec[ATTRCAT_NO_ATTRS];
    for(int i=0;i<rec[RELCAT_NO_ATTRIBUTES_INDEX].nVal;i++)
    {
        searchRec=linearSearch(ATTRCAT_RELID,name,oldRelationName,EQ);
        
        RecBuffer attrbuf(searchRec.block);
        attrbuf.getRecord(attrRec,searchRec.slot);
        strcpy(attrRec[ATTRCAT_REL_NAME_INDEX].sVal,newName);
        attrbuf.setRecord(attrRec,searchRec.slot);
    }
    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr;    // set relNameAttr to relName
    strcpy(relNameAttr.sVal,relName);
    RecId searchRec;
    char name[16]=RELCAT_ATTR_RELNAME;
    searchRec=linearSearch(RELCAT_RELID,name,relNameAttr,EQ);
    if(searchRec.block==-1 && searchRec.slot==-1)
    {
        return E_RELNOTEXIST;
    }

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    while (true) {
        
        searchRec=linearSearch(ATTRCAT_RELID,name,relNameAttr,EQ);
    
        if(searchRec.block==-1 && searchRec.slot==-1)
            break;
        RecBuffer attrbuf(searchRec.block);
      
        attrbuf.getRecord(attrCatEntryRecord,searchRec.slot);

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,oldName)==0)
        {
            attrToRenameRecId=searchRec;
            
        }

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName)==0)
        {
            return E_ATTREXIST;
        }
    }

    if(attrToRenameRecId.block==-1 && attrToRenameRecId.slot==-1)
    {
        return E_ATTRNOTEXIST;
    }

    RecBuffer attrbuf(attrToRenameRecId.block);
    attrbuf.getRecord(attrCatEntryRecord,attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName);
    attrbuf.setRecord(attrCatEntryRecord,attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record) {
    RelCatEntry relCatEntry;
    int ret = RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    if (ret != SUCCESS)
    return E_RELNOTOPEN;
    int blockNum = relCatEntry.firstBlk;

    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;

    int prevBlockNum = -1;

    while (blockNum != -1) {
        RecBuffer relBlock(blockNum);

        HeadInfo head;
        relBlock.getHeader(&head);

        unsigned char slotMap[head.numSlots];
        relBlock.getSlotMap(slotMap);

        for (int i = 0; i < head.numSlots; i++) {
            if (slotMap[i] == SLOT_UNOCCUPIED) {
                rec_id = {blockNum, i};
                break;
            }
        }

        if (rec_id.block != -1 && rec_id.slot != -1) {
            break;
        }

        prevBlockNum = blockNum;
        blockNum = head.rblock;
    }

    if (rec_id.block == -1 && rec_id.slot == -1)
    {
        if (relId == RELCAT_RELID) {
            return E_MAXRELATIONS;
        }

        RecBuffer newBlock;
        int ret = newBlock.getBlockNum();
        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }

        rec_id.block = ret;
        rec_id.slot = 0;

        HeadInfo head;
        head.blockType = REC;
        head.pblock = -1;
        head.lblock = prevBlockNum;
        head.rblock = -1;
        head.numEntries = 0;
        head.numSlots = numOfSlots;
        head.numAttrs = numOfAttributes;

        newBlock.setHeader(&head);

        unsigned char slotMap[numOfSlots];
        for (int i = 0; i < numOfSlots; i++) {
            slotMap[i] = SLOT_UNOCCUPIED;
        }
        newBlock.setSlotMap(slotMap);

        if (prevBlockNum != -1)
        {
            RecBuffer prevBlock(prevBlockNum);
            prevBlock.getHeader(&head);
            head.rblock = rec_id.block;
            prevBlock.setHeader(&head);
        }
        else
        {
            relCatEntry.firstBlk = rec_id.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }

        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    RecBuffer insRecBlock(rec_id.block);
    insRecBlock.setRecord(record, rec_id.slot);

    unsigned char slotMap[numOfSlots];
    insRecBlock.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    insRecBlock.setSlotMap(slotMap);

    HeadInfo head;
    insRecBlock.getHeader(&head);
    head.numEntries++;
    insRecBlock.setHeader(&head);

    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);


    int flag = SUCCESS;
    for (int attrOffset = 0; attrOffset < numOfAttributes; attrOffset++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrOffset, &attrCatEntry);

        int rootBlk = attrCatEntry.rootBlock;

        if (rootBlk != -1) {
            int retVal = BPlusTree::bPlusInsert(relId, attrCatEntry.attrName,
                                                record[attrOffset], rec_id);

            if (retVal == E_DISKFULL) {
                flag = E_INDEX_BLOCKS_RELEASED;
            }
        }
    }

    return flag;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    RecId recId;
    
    AttrCatEntry attrCatEntry;
    int ret=AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
     if(ret!=SUCCESS)
    {
        return ret;
    }
    int rootBlock=attrCatEntry.rootBlock;
    if(rootBlock==-1)
    { recId=BlockAccess::linearSearch(relId,attrName,attrVal,op);
    }
    else
    {
         recId=BPlusTree::bPlusSearch(relId,attrName,attrVal,op);
    }
    if(recId.block==-1 && recId.slot==-1)
    return E_NOTFOUND;
        RecBuffer rec(recId.block);
        rec.getRecord(record,recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    if (
        strcmp(relName, RELCAT_RELNAME) == 0 ||
        strcmp(relName, ATTRCAT_RELNAME) == 0
    ) {
		return E_NOTPERMITTED;
	}

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    char attrName[ATTR_SIZE];
    strcpy(attrName, RELCAT_ATTR_RELNAME);
    RecId recId = linearSearch(RELCAT_RELID, attrName, relNameAttr, EQ);

    if (recId.block == -1 && recId.slot == -1) {
        return E_RELNOTEXIST;
    }

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
  
    RecBuffer relCatBlock(recId.block);
    relCatBlock.getRecord(relCatEntryRecord, recId.slot);

    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal,
    numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    for (; firstBlock != -1;) {
        RecBuffer dataBlock(firstBlock);
        HeadInfo head;
        dataBlock.getHeader(&head);
        dataBlock.releaseBlock();
        firstBlock = head.rblock;
    }

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    int numberOfAttributesDeleted = 0;

    while(true) {
        RecId attrCatRecId;

        attrCatRecId = linearSearch(ATTRCAT_RELID, attrName, relNameAttr, EQ);

        if (attrCatRecId.block == -1 && attrCatRecId.slot == -1) {
            break;
        }

        numberOfAttributesDeleted++;

        RecBuffer attrCatBlock(attrCatRecId.block);
        HeadInfo head;
        attrCatBlock.getHeader(&head);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrCatRecord, attrCatRecId.slot);

        int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        unsigned char slotMap[head.numSlots];
        attrCatBlock.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        attrCatBlock.setSlotMap(slotMap);

        head.numEntries--;
        attrCatBlock.setHeader(&head);

        if (head.numEntries == 0) {
            RecBuffer leftBlock(head.lblock);
            HeadInfo leftHead;
            leftBlock.getHeader(&leftHead);
            leftHead.rblock = head.rblock;
            leftBlock.setHeader(&leftHead); 

            if (head.rblock != -1) {
                RecBuffer rightBlock(head.rblock);
                HeadInfo rightHead;
                rightBlock.getHeader(&rightHead);
                rightHead.lblock = head.lblock;
                rightBlock.setHeader(&rightHead);

            } else {
                relCatBlock.getRecord(relCatEntryRecord, recId.slot);
                relCatEntryRecord[RELCAT_LAST_BLOCK_INDEX].nVal = head.lblock;
                relCatBlock.setRecord(relCatEntryRecord, recId.slot);
            }

            attrCatBlock.releaseBlock();
        }

        if (rootBlock != -1) {
            BPlusTree::bPlusDestroy(rootBlock);
        }
    }

    relCatBlock = RecBuffer(RELCAT_BLOCK);
    HeadInfo head;
    relCatBlock.getHeader(&head);

    head.numEntries--;
    relCatBlock.setHeader(&head);

    unsigned char slotMap[head.numSlots];
    relCatBlock.getSlotMap(slotMap);
    slotMap[recId.slot] = SLOT_UNOCCUPIED;
    relCatBlock.setSlotMap(slotMap);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
    relCatEntry.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);

    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
    relCatEntry.numRecs -= numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);

    return SUCCESS;
}


int BlockAccess::project(int relId, Attribute *record) {
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);
    int block, slot;

    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
          RelCatEntry relEntry;
          RelCacheTable::getRelCatEntry(relId,&relEntry);
          block=relEntry.firstBlk;
          slot=0;
    }
    else
    {
        block=prevRecId.block;
        slot=prevRecId.slot+1;
    }

    while (block != -1)
    {
          RecBuffer relBuf(block);
          HeadInfo head;
          relBuf.getHeader(&head);
          unsigned char smap[head.numSlots];
          relBuf.getSlotMap(smap);

        if(slot>=head.numSlots)
        {
            block=head.rblock;
            slot=0;
        }
        else if (smap[slot]==SLOT_UNOCCUPIED)
        {
            slot++;
        }
        else {
            break;
        }
    }

    if (block == -1){
        return E_NOTFOUND;
    }

    RecId nextRecId{block, slot};

     RelCacheTable::setSearchIndex(relId,&nextRecId);
    
     RecBuffer rec(nextRecId.block);
     rec.getRecord(record,nextRecId.slot);

    return SUCCESS;
}
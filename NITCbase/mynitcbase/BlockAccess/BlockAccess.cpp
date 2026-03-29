#include "BlockAccess.h"
#include<stdio.h>
#include <cstring>
RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    // get the previous search index of the relation relId from the relation cache
    // (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);

    // let block and slot denote the record id of the record being currently checked
    int block,slot;
    // if the current search index record is invalid(i.e. both block and slot = -1)
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (no hits from previous search; search should start from the
        // first record itself)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId,&relCatEntry);
        block=relCatEntry.firstBlk;
        slot=0;
        // block = first record block of the relation
        // slot = 0
    }
    else
    {
        // (there is a hit from previous search; search should start from
        // the record next to the search index record)
        block=prevRecId.block;
        slot=prevRecId.slot+1;
        // block = search index's block
        // slot = search index's slot + 1
    }

    /* The following code searches for the next record in the relation
       that satisfies the given condition
       We start from the record id (block, slot) and iterate over the remaining
       records of the relation
    */
    AttrCatEntry attrBuf;
    AttrCacheTable::getAttrCatEntry(relId,attrName,&attrBuf);
    while (block != -1)
    {
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
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

        
        // get the record with id (block, slot) using RecBuffer::getRecord()
        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function

        

        // compare record's attribute value to the the given attrVal as below:
        /*
            firstly get the attribute offset for the attrName attribute
            from the attribute cache entry of the relation using
            AttrCacheTable::getAttrCatEntry()
        */
        


        /* use the attribute offset to get the value of the attribute from
           current record */

        int cmpVal;  // will store the difference between the attributes
        // set cmpVal using compareAttrs()
        cmpVal=compareAttrs(rec[attrBuf.offset],attrVal,attrBuf.attrType);
        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ) {
            /*
            set the search index in the relation cache as
            the record id of the record that satisfies the given condition
            (use RelCacheTable::setSearchIndex function)
            */
            RecId setIndex{block,slot};
            RelCacheTable::setSearchIndex(relId,&setIndex);
            return RecId{block, slot};
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    RelCacheTable::resetSearchIndex(relId);
    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute newRelationName;    // set newRelationName with newName
    strcpy(newRelationName.sVal,newName);

    // search the relation catalog for an entry with "RelName" = newRelationName
    RecId searchRec;
    char name[16]=RELCAT_ATTR_RELNAME;
    searchRec=linearSearch(RELCAT_RELID,name,newRelationName,EQ);
    if(searchRec.block!=-1 && searchRec.slot!=-1)
    {
        return E_RELEXIST;
    }
    // If relation with name newName already exists (result of linearSearch
    //                                               is not {-1, -1})
    //    return E_RELEXIST;

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */

    Attribute oldRelationName;    // set oldRelationName with oldName
    strcpy(oldRelationName.sVal,oldName);
    // search the relation catalog for an entry with "RelName" = oldRelationName
    searchRec=linearSearch(RELCAT_RELID,name,oldRelationName,EQ);
    if(searchRec.block==-1 && searchRec.slot==-1)
    {
        return E_RELNOTEXIST;
    }
    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;

    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
   RecBuffer recbuf(searchRec.block);
   
   Attribute rec[RELCAT_NO_ATTRS];
   recbuf.getRecord(rec,searchRec.slot);
    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    strcpy(rec[RELCAT_REL_NAME_INDEX].sVal,newName);
    recbuf.setRecord(rec,searchRec.slot);
    // set back the record value using RecBuffer.setRecord

    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    Attribute attrRec[ATTRCAT_NO_ATTRS];
    for(int i=0;i<rec[RELCAT_NO_ATTRIBUTES_INDEX].nVal;i++)
    {
        searchRec=linearSearch(ATTRCAT_RELID,name,oldRelationName,EQ);
        
        RecBuffer attrbuf(searchRec.block);
        attrbuf.getRecord(attrRec,searchRec.slot);
        strcpy(attrRec[ATTRCAT_REL_NAME_INDEX].sVal,newName);
        attrbuf.setRecord(attrRec,searchRec.slot);
    }
    //for i = 0 to numberOfAttributes :
    //    linearSearch on the attribute catalog for relName = oldRelationName
    //    get the record using RecBuffer.getRecord
    //
    //    update the relName field in the record to newName
    //    set back the record using RecBuffer.setRecord

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
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
    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */

    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    while (true) {
        // linear search on the attribute catalog for RelName = relNameAttr
        searchRec=linearSearch(ATTRCAT_RELID,name,relNameAttr,EQ);
        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;
        if(searchRec.block==-1 && searchRec.slot==-1)
            break;
        RecBuffer attrbuf(searchRec.block);
        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */
        
        attrbuf.getRecord(attrCatEntryRecord,searchRec.slot);

        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record
        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,oldName)==0)
        {
            attrToRenameRecId=searchRec;
            
        }
        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName)==0)
        {
            return E_ATTREXIST;
        }
    }

    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;
    if(attrToRenameRecId.block==-1 && attrToRenameRecId.slot==-1)
    {
        return E_ATTRNOTEXIST;
    }

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord
    RecBuffer attrbuf(attrToRenameRecId.block);
    attrbuf.getRecord(attrCatEntryRecord,attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName);
    attrbuf.setRecord(attrCatEntryRecord,attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record) {
    // get the relation catalog entry from relation cache
    // ( use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId,&relCatEntry);
    int blockNum = relCatEntry.firstBlk/* first record block of the relation (from the rel-cat entry)*/;

    // rec_id will be used to store where the new record will be inserted
    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk/* number of slots per record block */;
    int numOfAttributes = relCatEntry.numAttrs/* number of attributes of the relation */;

    int prevBlockNum = -1/* block number of the last element in the linked list = -1 */;

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR
        until the end of the list is reached
    */

    while (blockNum != -1) {
        // create a RecBuffer object for blockNum (using appropriate constructor!)
        RecBuffer recBuffer(blockNum);
        // get header of block(blockNum) using RecBuffer::getHeader() function
        HeadInfo head;
        recBuffer.getHeader(&head);
        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
        unsigned char slotMap[numOfSlots];
        recBuffer.getSlotMap(slotMap);
        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */
        int slot=0;
        while(slot<head.numSlots)
        {
            if(slotMap[slot]==SLOT_UNOCCUPIED)
            {
                rec_id={blockNum,slot};
                break;
            }
            slot++;
        }
        /* if a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */

        /* otherwise, continue to check the next block by updating the
           block numbers as follows:
              update prevBlockNum = blockNum
              update blockNum = header.rblock (next element in the linked
                                               list of record blocks)
        */
       prevBlockNum=blockNum;
       blockNum=head.rblock;
    }

    //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
    if(rec_id.block==-1 && rec_id.slot==-1)
    {
        // if relation is RELCAT, do not allocate any more blocks
        //     return E_MAXRELATIONS;
        if(relId==0)
            return E_MAXRELATIONS;
        RecBuffer newBlock;
        int ret=newBlock.getBlockNum();
        // Otherwise,
        // get a new record block (using the appropriate RecBuffer constructor!)
        // get the block number of the newly allocated block
        // (use BlockBuffer::getBlockNum() function)
        // let ret be the return value of getBlockNum() function call
        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }
        rec_id.block=ret;
        rec_id.slot=0;
        // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
        HeadInfo header;
        header.blockType=REC;
        header.pblock=-1;
        header.lblock=prevBlockNum;
        header.rblock=-1;
        header.numEntries=0;
        header.numSlots=numOfSlots;
        header.numAttrs=numOfAttributes;
        newBlock.setHeader(&header);

        /*
            set the header of the new record block such that it links with
            existing record blocks of the relation
            set the block's header as follows:
            blockType: REC, pblock: -1
            lblock
                  = -1 (if linked list of existing record blocks was empty
                         i.e this is the first insertion into the relation)
                  = prevBlockNum (otherwise),
            rblock: -1, numEntries: 0,
            numSlots: numOfSlots, numAttrs: numOfAttributes
            (use BlockBuffer::setHeader() function)
        */
        unsigned char smap[numOfSlots];
        for(int i=0;i<numOfSlots;i++)
        {
            smap[i]=SLOT_UNOCCUPIED;
        }
        /*
            set block's slot map with all slots marked as free
            (i.e. store SLOT_UNOCCUPIED for all the entries)
            (use RecBuffer::setSlotMap() function)
        */
        newBlock.setSlotMap(smap);
        // if prevBlockNum != -1
        if(prevBlockNum!=-1)
        {
            RecBuffer prev(prevBlockNum);
            prev.getHeader(&header);
            header.rblock=rec_id.block;
            prev.setHeader(&header);
            // create a RecBuffer object for prevBlockNum
            // get the header of the block prevBlockNum and
            // update the rblock field of the header to the new block
            // number i.e. rec_id.block
            // (use BlockBuffer::setHeader() function)
        }
        else
        {
            relCatEntry.firstBlk=rec_id.block;
            RelCacheTable::setRelCatEntry(relId,&relCatEntry);
            // update first block field in the relation catalog entry to the
            // new block (using RelCacheTable::setRelCatEntry() function)
        }
        relCatEntry.lastBlk=rec_id.block;
        RelCacheTable::setRelCatEntry(relId,&relCatEntry);
        // update last block field in the relation catalog entry to the
        // new block (using RelCacheTable::setRelCatEntry() function)
    }
    RecBuffer recBlock(rec_id.block);
    recBlock.setRecord(record,rec_id.slot);
    unsigned char slotmap[numOfSlots];
    recBlock.getSlotMap(slotmap);
    // create a RecBuffer object for rec_id.block
    // insert the record into rec_id'th slot using RecBuffer.setRecord())
    slotmap[rec_id.slot]=SLOT_OCCUPIED;
    /* update the slot map of the block by marking entry of the slot to
       which record was inserted as occupied) */
    // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
    // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)
    recBlock.setSlotMap(slotmap);
    HeadInfo header2;
    recBlock.getHeader(&header2);
    header2.numEntries++;
    recBlock.setHeader(&header2);
    // increment the numEntries field in the header of the block to
    // which record was inserted
    // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId,&relCatEntry);
    // Increment the number of records field in the relation cache entry for
    // the relation. (use RelCacheTable::setRelCatEntry function)

    return SUCCESS;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* search for the record id (recid) corresponding to the attribute with
    attribute name attrName, with value attrval and satisfying the condition op
    using linearSearch() */
     recId=BlockAccess::linearSearch(relId,attrName,attrVal,op);
    // if there's no record satisfying the given condition (recId = {-1, -1})
    //    return E_NOTFOUND;
    if(recId.block==-1 && recId.slot==-1)
    return E_NOTFOUND;
        RecBuffer rec(recId.block);
        rec.getRecord(record,recId.slot);
    /* Copy the record with record id (recId) to the record buffer (record)
       For this Instantiate a RecBuffer class object using recId and
       call the appropriate method to fetch the record
    */

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    if(strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0)
        return E_NOTPERMITTED;
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr; // (stores relName as type union Attribute)
    // assign relNameAttr.sVal = relName
       strcpy(relNameAttr.sVal,relName);
    //  linearSearch on the relation catalog for RelName = relNameAttr
      RecId recId;
      char name[16]=RELCAT_ATTR_RELNAME;
      recId=BlockAccess::linearSearch(RELCAT_RELID,name,relNameAttr,EQ);
    // if the relation does not exist (linearSearch returned {-1, -1})
    //     return E_RELNOTEXIST
     if(recId.block==-1 && recId.slot==-1)
     return E_RELNOTEXIST;
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    /* store the relation catalog record corresponding to the relation in
       relCatEntryRecord using RecBuffer.getRecord */
     RecBuffer relCatBuffer(recId.block);
     relCatBuffer.getRecord(relCatEntryRecord,recId.slot);
     int firstBlock=relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
     int numAttrs=relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    /* get the first record block of the relation (firstBlock) using the
       relation catalog entry record */
    /* get the number of attributes corresponding to the relation (numAttrs)
       using the relation catalog entry record */

    /*
     Delete all the record blocks of the relation
    */
    // for each record block of the relation:
    //     get block header using BlockBuffer.getHeader
    //     get the next block from the header (rblock)
    //     release the block using BlockBuffer.releaseBlock
    //
    //     Hint: to know if we reached the end, check if nextBlock = -1
  int currentBlock=firstBlock;
  while(currentBlock!=-1)
  {
    RecBuffer rec(currentBlock);
    HeadInfo head;
    rec.getHeader(&head);
    int nextBlock=head.rblock;
    rec.releaseBlock();
    currentBlock=nextBlock;
  }
    /***
        Deleting attribute catalog entries corresponding the relation and index
        blocks corresponding to the relation with relName on its attributes
    ***/

    // reset the searchIndex of the attribute catalog
      RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    int numberOfAttributesDeleted = 0;

    while(true) {
        RecId attrCatRecId;
        // attrCatRecId = linearSearch on attribute catalog for RelName = relNameAttr
          attrCatRecId=linearSearch(ATTRCAT_RELID,(char*)RELCAT_ATTR_RELNAME,relNameAttr,EQ);
          if(attrCatRecId.block==-1 && attrCatRecId.slot==-1)
          break;
        // if no more attributes to iterate over (attrCatRecId == {-1, -1})
        //     break;
        numberOfAttributesDeleted++;

        // create a RecBuffer for attrCatRecId.block
        // get the header of the block
        // get the record corresponding to attrCatRecId.slot
         RecBuffer attrCatBuffer(attrCatRecId.block);
         HeadInfo head1;
         attrCatBuffer.getHeader(&head1);
         Attribute record[head1.numAttrs];
         attrCatBuffer.getRecord(record,attrCatRecId.slot);
        // declare variable rootBlock which will be used to store the root
        // block field from the attribute catalog record.

        int rootBlock;/* get root block from the record */
        // (This will be used later to delete any indexes if it exists)

        // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
        // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
        unsigned char smap[head1.numSlots];
        attrCatBuffer.getSlotMap(smap);
        smap[attrCatRecId.slot]=SLOT_UNOCCUPIED;
        attrCatBuffer.setSlotMap(smap);
        /* Decrement the numEntries in the header of the block corresponding to
           the attribute catalog entry and then set back the header
           using RecBuffer.setHeader */
           head1.numEntries--;
           attrCatBuffer.setHeader(&head1);
        /* If number of entries become 0, releaseBlock is called after fixing
           the linked list.
        */
        if (head1.numEntries==0/*   header.numEntries == 0  */) {

            /* Standard Linked List Delete for a Block
               Get the header of the left block and set it's rblock to this
               block's rblock
            */
             int left=head1.lblock;
             int right=head1.rblock;
            // create a RecBuffer for lblock and call appropriate methods
            if (left!=-1/* header.rblock != -1 */) {
                RecBuffer prevBlock(left);
                HeadInfo head2;
                prevBlock.getHeader(&head2);
                 head2.rblock=right;
                 prevBlock.setHeader(&head2);
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                // create a RecBuffer for rblock and call appropriate methods

            }
            if (right!=-1/* header.rblock != -1 */) {
                RecBuffer nextBlock(right);
                HeadInfo head2;
                nextBlock.getHeader(&head2);
                 head2.lblock=left;
                 nextBlock.setHeader(&head2);
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                // create a RecBuffer for rblock and call appropriate methods

            }

            // (Since the attribute catalog will never be empty(why?), we do not
            //  need to handle the case of the linked list becoming empty - i.e
            //  every block of the attribute catalog gets released.)

            // call releaseBlock()
            attrCatBuffer.releaseBlock();
        }
    }

    /*** Delete the entry corresponding to the relation from relation catalog ***/
    // Fetch the header of Relcat block
     HeadInfo relCatHead;
     relCatBuffer.getHeader(&relCatHead);
     unsigned char smap1[relCatHead.numSlots];
     relCatBuffer.getSlotMap(smap1);
     smap1[recId.slot]=SLOT_UNOCCUPIED;
     relCatBuffer.setSlotMap(smap1);
     relCatHead.numEntries--;
     relCatBuffer.setHeader(&relCatHead);
    /* Decrement the numEntries in the header of the block corresponding to the
       relation catalog entry and set it back */

    /* Get the slotmap in relation catalog, update it by marking the slot as
       free(SLOT_UNOCCUPIED) and set it back. */

    /*** Updating the Relation Cache Table ***/
    /** Update relation catalog record entry (number of records in relation
        catalog is decreased by 1) **/
        RelCatEntry relbuf;
        RelCacheTable::getRelCatEntry(RELCAT_RELID,&relbuf);
        relbuf.numRecs--;
        RelCacheTable::setRelCatEntry(RELCAT_RELID,&relbuf);
    // Get the entry corresponding to relation catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    /** Update attribute catalog entry (number of records in attribute catalog
        is decreased by numberOfAttributesDeleted) **/
    // i.e., #Records = #Records - numberOfAttributesDeleted
     RelCatEntry attrbuf;
     RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&attrbuf);
     attrbuf.numRecs-=numberOfAttributesDeleted;
     RelCacheTable::setRelCatEntry(ATTRCAT_RELID,&attrbuf);
    // Get the entry corresponding to attribute catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    return SUCCESS;
}

int BlockAccess::project(int relId, Attribute *record) {
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);
    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block, slot;

    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (new project operation. start from beginning)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
          RelCatEntry relEntry;
          RelCacheTable::getRelCatEntry(relId,&relEntry);
          block=relEntry.firstBlk;
          slot=0;
        // block = first record block of the relation
        // slot = 0
    }
    else
    {
        // (a project/search operation is already in progress)
           block=prevRecId.block;
           slot=prevRecId.slot+1;
        // block = previous search index's block
        // slot = previous search index's slot + 1
    }


    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1)
    {
        // create a RecBuffer object for block (using appropriate constructor!)
          RecBuffer relBuf(block);
          HeadInfo head;
          relBuf.getHeader(&head);
          unsigned char smap[head.numSlots];
          relBuf.getSlotMap(smap);
        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function

        if(slot>=head.numSlots/* slot >= the number of slots per block*/)
        {
            // (no more slots in this block)
            // update block = right block of block
            // update slot = 0
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
            block=head.rblock;
            slot=0;
        }
        else if (smap[slot]==SLOT_UNOCCUPIED/* slot is free */)
        { // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)
               slot++;
            // increment slot
        }
        else {
            // (the next occupied slot / record has been found)
            break;
        }
    }

    if (block == -1){
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId{block, slot};

    // set the search index to nextRecId using RelCacheTable::setSearchIndex
     RelCacheTable::setSearchIndex(relId,&nextRecId);
    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
     RecBuffer rec(nextRecId.block);
     rec.getRecord(record,nextRecId.slot);

    return SUCCESS;
}
#include "BPlusTree.h"

#include <cstring>
extern int comparisionCount;

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    IndexId searchIndex;

    AttrCacheTable::getSearchIndex(relId,attrName,&searchIndex);
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
    int type=attrCatEntry.attrType;
    int block, index;

    if (searchIndex.block==-1 && searchIndex.index==-1) {
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block==-1) {
            return RecId{-1, -1};
        }

    } else {
        block = searchIndex.block;
        index = searchIndex.index + 1;

        IndLeaf leaf(block);

        HeadInfo leafHead;
        leaf.getHeader(&leafHead);
        if (index >= leafHead.numEntries) {
              block=leafHead.rblock;
              index=0;

            if (block == -1) {
                return RecId{-1, -1};
            }
        }
    }

 if(searchIndex.block==-1 && searchIndex.index==-1){
    while(StaticBuffer::getStaticBlockType(block)==IND_INTERNAL) {
        IndInternal internalBlk(block);

        HeadInfo intHead;
       internalBlk.getHeader(&intHead);

        InternalEntry intEntry;

        if (op==NE || op==LT || op==LE) {
            internalBlk.getEntry(&intEntry,0);
            block = intEntry.lChild;

        } else {
               bool found=false;
               for(int i=0;i<intHead.numEntries;i++)
               {
                internalBlk.getEntry(&intEntry,i);
                int cmpVal=compareAttrs(intEntry.attrVal,attrVal,type);
                comparisionCount++;
                if((op==EQ && cmpVal>=0) || (op==GT && cmpVal>0) || (op==GE && cmpVal>=0))
                {
                    found=true;
                    break;
                }
               }
            if (found==true) {
                block = intEntry.lChild;

            } else {
                block =intEntry.rChild;
                }
            }
        }
    }

    while (block != -1) {
        IndLeaf leafBlk(block);
        HeadInfo leafHead;

           leafBlk.getHeader(&leafHead);
        Index leafEntry;

        while (index<leafHead.numEntries) {
            leafBlk.getEntry(&leafEntry,index);
            int cmpVal = compareAttrs(leafEntry.attrVal,attrVal,type);
            comparisionCount++;

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {
                 searchIndex.block=block;
                 searchIndex.index=index;
                 AttrCacheTable::setSearchIndex(relId,attrName,&searchIndex);
                 
                 return RecId{leafEntry.block,leafEntry.slot};
            } else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) {
                return RecId{-1,-1};
            }

            ++index;
        }

        if (op != NE) {
            break;
        }

        block=leafHead.rblock;
        index=0;
    }
    return RecId{-1,-1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {

    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }

    AttrCatEntry attrCatEntry;
    int retVal = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    if (retVal != SUCCESS) {
        return retVal;
    }
    
    if (attrCatEntry.rootBlock != -1) {
        return SUCCESS;
    }

    IndLeaf rootBlockBuf;

    int rootBlock = rootBlockBuf.getBlockNum();

    if (rootBlock == E_DISKFULL) {
        return E_DISKFULL;
    }
    attrCatEntry.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    RelCatEntry relCatEntry;

    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int block = relCatEntry.firstBlk;
    while (block != -1) {
        RecBuffer recBlk(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];

        recBlk.getSlotMap(slotMap);

        for (int i = 0; i < relCatEntry.numSlotsPerBlk; i++) {

            if (slotMap[i] == SLOT_UNOCCUPIED) {
                continue;
            }
            
            Attribute record[relCatEntry.numAttrs];
            recBlk.getRecord(record, i);

            RecId recId{block, i};

            retVal = bPlusInsert(relId, attrName, record[attrCatEntry.offset], recId);

            if (retVal == E_DISKFULL) {
                return E_DISKFULL;
            }
        }

        HeadInfo head;
        recBlk.getHeader(&head);

        block = head.rblock;
    }

    return SUCCESS;
}


int BPlusTree::bPlusDestroy(int rootBlockNum) {
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF) {
        IndLeaf indLeafBlk(rootBlockNum);

        indLeafBlk.releaseBlock();

        return SUCCESS;

    } else if (type == IND_INTERNAL) {
        IndInternal indIntBlk(rootBlockNum);

        HeadInfo head;
        indIntBlk.getHeader(&head);

        InternalEntry entry;
        indIntBlk.getEntry(&entry, 0);
        if (entry.lChild != -1)
        bPlusDestroy(entry.lChild);
        for (int i = 0; i < head.numEntries; i++) {
            indIntBlk.getEntry(&entry, i);
            if (entry.rChild != -1)
            bPlusDestroy(entry.rChild);
        }

        indIntBlk.releaseBlock();

        return SUCCESS;

    } else {
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId) {
    AttrCatEntry attrCatEntry;
    int retVal = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    if (retVal != SUCCESS) {
        return retVal;
    }

    int blockNum = attrCatEntry.rootBlock;

    if (blockNum == -1) {   
        return E_NOINDEX;
    }

    int leafBlkNum = findLeafToInsert(blockNum, attrVal, attrCatEntry.attrType);

    Index leafEntry{attrVal, recId.block, recId.slot};
    retVal = insertIntoLeaf(relId, attrName, leafBlkNum, leafEntry);

    if (retVal == E_DISKFULL) {
        bPlusDestroy(blockNum);

        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    int blockNum = rootBlock;

    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {
        IndInternal indIntBlk(blockNum);

        HeadInfo head;
        indIntBlk.getHeader(&head);

        int found = 0;
        InternalEntry entry;
        for (int i = 0; i < head.numEntries; i++) {
            indIntBlk.getEntry(&entry, i);

            int cmpVal = compareAttrs(entry.attrVal, attrVal, attrType);

            if (cmpVal >= 0) {
                found = 1;
                break;
            }
        }

        if (!found) {
            blockNum = entry.rChild;

        } else {
            blockNum = entry.lChild;
        }
    }

    return blockNum;
}


int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndLeaf indLeafBlk(blockNum);

    HeadInfo blockHeader;
    indLeafBlk.getHeader(&blockHeader);

    Index indices[blockHeader.numEntries + 1];

    for (int i = 0; i < blockHeader.numEntries; i++) {
        indLeafBlk.getEntry(&indices[i], i);
    }

    int i, retVal;
    for (i = 0; i < blockHeader.numEntries; i++) {

        retVal = compareAttrs(indices[i].attrVal, indexEntry.attrVal, attrCatEntry.attrType);
        if (retVal > 0) {
            for (int j = blockHeader.numEntries; j > i; j--) {
                indices[j] = indices[j - 1];
            }
            indices[i] = indexEntry;
            break;
        }
    }
    if (i == blockHeader.numEntries) {
        indices[i] = indexEntry;
    }

    if (blockHeader.numEntries != MAX_KEYS_LEAF) {
        blockHeader.numEntries++;
        indLeafBlk.setHeader(&blockHeader);

        for (int i = 0; i < blockHeader.numEntries; i++) {
            indLeafBlk.setEntry(&indices[i], i);
        }

        return SUCCESS;
    }

    int newRightBlk = splitLeaf(blockNum, indices);

    if (newRightBlk == E_DISKFULL) {
        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1) {
        InternalEntry newIntEntry;
        newIntEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        newIntEntry.lChild = blockNum;
        newIntEntry.rChild = newRightBlk;

        return insertIntoInternal(relId, attrName, blockHeader.pblock, newIntEntry);

    } else {
        return createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);
    }

}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
    IndLeaf rightBlk;

    IndLeaf leftBlk(leafBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leafBlockNum;

    if (rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;

    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    if (rightBlkHeader.rblock != -1) {
        IndLeaf nextBlk(rightBlkHeader.rblock);
        HeadInfo nextHeader;
        nextBlk.getHeader(&nextHeader);
        nextHeader.lblock = rightBlkNum;
        nextBlk.setHeader(&nextHeader);
    }
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    for (int i = 0; i < leftBlkHeader.numEntries; i++) {
        leftBlk.setEntry(&indices[i], i);
    }
    for (int i = 0; i < rightBlkHeader.numEntries; i++) {
        rightBlk.setEntry(&indices[i + leftBlkHeader.numEntries], i);
    }

    return rightBlkNum;
}


int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal intBlk(intBlockNum);

    HeadInfo blockHeader;

    intBlk.getHeader(&blockHeader);

    InternalEntry internalEntries[blockHeader.numEntries + 1];
    int i, retVal;
    for (i = 0; i < blockHeader.numEntries; i++) {
        intBlk.getEntry(&internalEntries[i], i);
    }
    
    for (i = 0; i < blockHeader.numEntries; i++) {
        retVal = compareAttrs(internalEntries[i].attrVal, intEntry.attrVal, attrCatEntry.attrType);
        if (retVal > 0) {
            for (int j = blockHeader.numEntries; j > i; j--) {
                internalEntries[j] = internalEntries[j - 1];
            }

            internalEntries[i] = intEntry;
            break;
        }
    }
    if (i == blockHeader.numEntries) {
        internalEntries[i] = intEntry;
    }

    if (i < blockHeader.numEntries) {
        internalEntries[i + 1].lChild = intEntry.rChild;
    }
    if (i > 0) {
        internalEntries[i - 1].rChild = intEntry.lChild;
    }

    if (blockHeader.numEntries != MAX_KEYS_INTERNAL) {
        blockHeader.numEntries++;
        intBlk.setHeader(&blockHeader);

        for (i = 0; i < blockHeader.numEntries; i++) {
            intBlk.setEntry(&internalEntries[i], i);
        }

        return SUCCESS;
    }

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL) {

        bPlusDestroy(intEntry.rChild);
        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1) {
        InternalEntry newIntEntry;

        newIntEntry.lChild = intBlockNum;
        newIntEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        newIntEntry.rChild = newRightBlk;

        return insertIntoInternal(relId, attrName, blockHeader.pblock, newIntEntry);

    } else {
        return createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);
    }


}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
    IndInternal rightBlk;

    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();

    int leftBlkNum = intBlockNum;
    
    if (rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    leftBlk.setHeader(&leftBlkHeader);

    for (int i = 0; i < leftBlkHeader.numEntries; i++) {
        leftBlk.setEntry(&internalEntries[i], i);
    }
    for (int i = 0; i < rightBlkHeader.numEntries; i++) {
        rightBlk.setEntry(&internalEntries[i + MIDDLE_INDEX_INTERNAL + 1], i);
    }

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild); 

    BlockBuffer firstBlk (internalEntries[MIDDLE_INDEX_INTERNAL + 1].lChild);

    HeadInfo firstChildHeader;
    firstBlk.getHeader(&firstChildHeader);
    firstChildHeader.pblock = rightBlkNum;
    firstBlk.setHeader(&firstChildHeader);

    for (int i = 0; i < MIDDLE_INDEX_INTERNAL; i++)
    {
        BlockBuffer childBlk(internalEntries[i + MIDDLE_INDEX_INTERNAL + 1].rChild);

        HeadInfo childHeader;
        childBlk.getHeader(&childHeader);
        childHeader.pblock = rightBlkNum;
        childBlk.setHeader(&childHeader);
    }

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal newRootBlk;

    int newRootBlkNum = newRootBlk.getBlockNum();

    if (newRootBlkNum == E_DISKFULL) {
        bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    HeadInfo newRootBlkHeader;
    newRootBlk.getHeader(&newRootBlkHeader);
    newRootBlkHeader.numEntries = 1;
    newRootBlk.setHeader(&newRootBlkHeader);

    InternalEntry newRootEntry;
    newRootEntry.lChild = lChild;
    newRootEntry.attrVal = attrVal;
    newRootEntry.rChild = rChild;
    newRootBlk.setEntry(&newRootEntry, 0);

    BlockBuffer lChildBlk(lChild);
    BlockBuffer rChildBlk(rChild);
    
    HeadInfo lChildHeader, rChildHeader;
    lChildBlk.getHeader(&lChildHeader);
    rChildBlk.getHeader(&rChildHeader);

    lChildHeader.pblock = newRootBlkNum;
    rChildHeader.pblock = newRootBlkNum;

    lChildBlk.setHeader(&lChildHeader);
    rChildBlk.setHeader(&rChildHeader);

    attrCatEntry.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    return SUCCESS;
}

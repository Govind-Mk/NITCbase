#include "BlockBuffer.h"
#include<stdio.h>
#include <cstdlib>
#include <cstring>
#include <cstdint>

BlockBuffer::BlockBuffer(int blockNum) {
  this->blockNum=blockNum;
}

BlockBuffer::BlockBuffer(char blockType){
    int type;
    if(blockType=='R')
    {
      type=REC;
    }
    else if(blockType=='I')
    {
      type=IND_INTERNAL;
    }
    else if(blockType=='L')
    {
      type=IND_LEAF;
    }
    else
    {
      type=UNUSED_BLK;
    }
    int ret=getFreeBlock(type);
    this->blockNum=ret;
  
    if(ret<0 || ret>=DISK_BLOCKS)
      return;
}

RecBuffer::RecBuffer() : BlockBuffer('R'){}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType){}

IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum){}

IndInternal::IndInternal() : IndBuffer('I') {}

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

IndLeaf::IndLeaf() : IndBuffer('L') {}
                                       
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}


int BlockBuffer::getHeader(struct HeadInfo *head) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  memcpy(&head->blockType, bufferPtr + 0, 4);
  memcpy(&head->pblock, bufferPtr + 4, 4);
  memcpy(&head->numSlots, bufferPtr + 24, 4);
  memcpy(&head->numEntries, bufferPtr+16, 4);
  memcpy(&head->numAttrs,bufferPtr+20 , 4);
  memcpy(&head->rblock,bufferPtr+12 , 4);
  memcpy(&head->lblock,bufferPtr+8 , 4);

  return SUCCESS;
}


int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  struct HeadInfo head;

  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
    
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr+HEADER_SIZE+slotCount+slotNum*recordSize;

  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}


int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
  if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    } 
  if (bufferNum != E_BLOCKNOTINBUFFER)
  {
    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
      if(StaticBuffer::metainfo[i].free==false)
      {
        StaticBuffer::metainfo[i].timeStamp+=1;
      }
    }
    StaticBuffer::metainfo[bufferNum].timeStamp=0;
  }
  else{
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  getHeader(&head);

  int slotCount = head.numSlots;

  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  memcpy(slotMap,slotMapInBuffer,slotCount);

  return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    double diff;
    if(attrType == STRING)
        diff = strcmp(attr1.sVal, attr2.sVal);
    else
        diff = attr1.nVal - attr2.nVal;

    if(diff > 0) return 1;
    if(diff < 0) return -1;
    return 0;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  struct HeadInfo head;

  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
  if(slotNum<0 || slotCount<=slotNum)
  {
    return E_OUTOFBOUND;
  }
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr+HEADER_SIZE+slotCount+slotNum*recordSize;

  memcpy(slotPointer,rec, recordSize);
  
  StaticBuffer::setDirtyBit(this->blockNum);
  return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head){
    unsigned char *bufferPtr;
    int ret=loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret!=SUCCESS)
      return ret;
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    bufferHeader->lblock=head->lblock;
    bufferHeader->numAttrs=head->numAttrs;
    bufferHeader->numEntries=head->numEntries;
    bufferHeader->numSlots=head->numSlots;
    bufferHeader->pblock=head->pblock;
    bufferHeader->rblock=head->rblock;

    ret=StaticBuffer::setDirtyBit(this->blockNum);
    if(ret!=SUCCESS)
      return ret;

    return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType){
    unsigned char *bufferPtr;
    int ret=loadBlockAndGetBufferPtr(&bufferPtr);
    
    if(ret!=SUCCESS)
      return ret;
    
    *((int32_t *)bufferPtr)=blockType;
    
    StaticBuffer::blockAllocMap[this->blockNum]=blockType;

    ret=StaticBuffer::setDirtyBit(this->blockNum);

    if(ret!=SUCCESS)
      return ret;
    return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType){
    int freeBlock=-1;
    for(int i=0;i<DISK_BLOCKS;i++)
    {
      if(StaticBuffer::blockAllocMap[i]==UNUSED_BLK)
      {
        freeBlock=i;
        break;
      }
    }
    
    if(freeBlock==-1)
    {
      return E_DISKFULL;
    }
   
    this->blockNum=freeBlock;
   
    int freeBuffer=StaticBuffer::getFreeBuffer(freeBlock);
 
    struct HeadInfo head;
    head.pblock=-1;
    head.lblock=-1;
    head.rblock=-1;
    head.numEntries=0;
    head.numAttrs=0;
    head.numSlots=0;
    setHeader(&head);
    setBlockType(blockType);

    return freeBlock;
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) {
      return ret;
    }

    struct HeadInfo head;
    getHeader(&head);
    int numSlots = head.numSlots;

    unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMapInBuffer,slotMap,numSlots);

    ret=StaticBuffer::setDirtyBit(this->blockNum);
    return ret;
}

int BlockBuffer::getBlockNum(){
  return this->blockNum;
}

void BlockBuffer::releaseBlock(){
    if(this->blockNum==INVALID_BLOCKNUM)
    return;

       int bufferNum=StaticBuffer::getBufferNum(this->blockNum);
        if(bufferNum!=E_BLOCKNOTINBUFFER)
        {
          StaticBuffer::metainfo[bufferNum].free=true;
          StaticBuffer::blockAllocMap[this->blockNum]=UNUSED_BLK;
          this->blockNum=INVALID_BLOCKNUM;
        }
        else{
          StaticBuffer::blockAllocMap[this->blockNum]=UNUSED_BLK;
          this->blockNum=INVALID_BLOCKNUM;
        }

       return;
}

int IndInternal::getEntry(void *ptr, int indexNum) {
     if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
    {
        return E_OUTOFBOUND;
    }
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
    memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
    memcpy(&(internalEntry->rChild), entryPtr + 20, 4);

    return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum) {
   if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
    {
        return E_OUTOFBOUND;
    }
    unsigned char *bufferPtr;

     int ret=loadBlockAndGetBufferPtr(&bufferPtr);

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}

int IndInternal::setEntry(void *ptr, int indexNum) {
	if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
		return E_OUTOFBOUND;
	}

    unsigned char *bufferPtr;

	int ret = loadBlockAndGetBufferPtr(&bufferPtr);

	if (ret != SUCCESS) {
		return ret;
	}

    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * (sizeof(int32_t) + ATTR_SIZE));

    memcpy(entryPtr, &(internalEntry->lChild), 4);
    memcpy(entryPtr + 4, &(internalEntry->attrVal), ATTR_SIZE);
    memcpy(entryPtr + 20, &(internalEntry->rChild), 4);

	return StaticBuffer::setDirtyBit(this->blockNum);
}

int IndLeaf::setEntry(void *ptr, int indexNum) {
	  if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
		  return E_OUTOFBOUND;
	  }

    unsigned char *bufferPtr;

	  int ret = loadBlockAndGetBufferPtr(&bufferPtr);

	  if (ret != SUCCESS) {
		  return ret;
	  }

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy(entryPtr, (struct Index *)ptr, LEAF_ENTRY_SIZE);

	return StaticBuffer::setDirtyBit(this->blockNum);
}
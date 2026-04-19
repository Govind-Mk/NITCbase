#include "StaticBuffer.h"
#include<string.h>

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];
StaticBuffer::StaticBuffer() {
  unsigned char buffer[BLOCK_SIZE];
  for(int i=0;i<4;i++)
  {
    Disk::readBlock(buffer,i);
    memcpy(blockAllocMap+i*BLOCK_SIZE,buffer,BLOCK_SIZE);
  }
  for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++) {
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty = false;
    metainfo[bufferIndex].timeStamp = -1;
    metainfo[bufferIndex].blockNum = -1;
  }
}


StaticBuffer::~StaticBuffer() {
  unsigned char buffer[BLOCK_SIZE];
  for(int i=0;i<4;i++)
  {
    memcpy(buffer,blockAllocMap+i*BLOCK_SIZE,BLOCK_SIZE);
    Disk::writeBlock(buffer,i);
  }
   unsigned char *bufferptr;
   for(int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++)
   {
    if(metainfo[bufferIndex].free==false && metainfo[bufferIndex].dirty==true)
    {
      bufferptr=blocks[bufferIndex];
      Disk::writeBlock(bufferptr,metainfo[bufferIndex].blockNum);
    }
   }
}


int StaticBuffer::getFreeBuffer(int blockNum){
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
      return E_OUTOFBOUND;
    }
    
    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
      if(metainfo[i].free==false)
      {
        metainfo[i].timeStamp+=1;
      }
    }
    
    int bufferNum;
    int allocatedBuffer,max=0;
    
    for(allocatedBuffer=0;allocatedBuffer<BUFFER_CAPACITY;allocatedBuffer++)
    {
      if(metainfo[max].timeStamp<metainfo[allocatedBuffer].timeStamp)
      {
        max=allocatedBuffer;
      }
      if(metainfo[allocatedBuffer].free==true)
      {
        break;
      }
    }
    
    if(allocatedBuffer!=BUFFER_CAPACITY)
    {
      bufferNum=allocatedBuffer;
    }
    else 
    {
      if(metainfo[max].dirty==true)
        Disk::writeBlock(blocks[max],metainfo[max].blockNum);
      bufferNum=max;
    }
      metainfo[bufferNum].blockNum=blockNum;
      metainfo[bufferNum].dirty=false;
      metainfo[bufferNum].free=false;
      metainfo[bufferNum].timeStamp=0;
      
    return bufferNum;
}

int StaticBuffer::getBufferNum(int blockNum) {
    if(blockNum<0 || blockNum>=DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }
  
    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
        if(metainfo[i].free==false && metainfo[i].blockNum==blockNum)
        {
            return i;
        }
    }
  
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
  int bufferNum = getBufferNum(blockNum);
    
  if(bufferNum==E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;
    
  if(bufferNum==E_OUTOFBOUND)
    return E_OUTOFBOUND;
  else{
    metainfo[bufferNum].dirty=true;
    }
    return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum){
  if(blockNum<0 || blockNum>=DISK_BLOCKS)
    {
      return E_OUTOFBOUND;
    }
    return (int)blockAllocMap[blockNum];
}
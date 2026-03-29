#include "Algebra.h"
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <cstdio>
using namespace std;

bool isNumber(char *str) {
  int len;
  float ignore;
  /*
    sscanf returns the number of elements read, so if there is no float matching
    the first %f, ret will be 0, else it'll be 1

    %n gets the number of characters read. this scanf sequence will read the
    first float ignoring all the whitespace before and after. and the number of
    characters read that far will be stored in len. if len == strlen(str), then
    the string only contains a float with/without whitespace. else, there's other
    characters.
  */
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}

/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into.
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTOPEN) {
    return E_RELNOTOPEN;
  }

  AttrCatEntry attrCatEntry;
  // get the attribute catalog entry for attr, using AttrCacheTable::getAttrcatEntry()
  //    return E_ATTRNOTEXIST if it returns the error
  if(AttrCacheTable::getAttrCatEntry(srcRelId,attr,&attrCatEntry)!=SUCCESS)
  {
    return E_ATTRNOTEXIST;
  }


  /*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/
  int type = attrCatEntry.attrType;
  Attribute attrVal;
  if (type == NUMBER) {
    if (isNumber(strVal)) {       // the isNumber() function is implemented below
      attrVal.nVal = atof(strVal);
    } else {
      return E_ATTRTYPEMISMATCH;
    }
  } else if (type == STRING) {
    strcpy(attrVal.sVal, strVal);
  }

  // /*** Selecting records from the source relation ***/

  // // Before calling the search function, reset the search to start from the first hit
  // // using RelCacheTable::resetSearchIndex()
  // RelCacheTable::resetSearchIndex(srcRelId);
  // RelCatEntry relCatEntry;
  // // get relCatEntry using RelCacheTable::getRelCatEntry()
  // RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);
  // /************************
  // The following code prints the contents of a relation directly to the output
  // console. Direct console output is not permitted by the actual the NITCbase
  // specification and the output can only be inserted into a new relation. We will
  // be modifying it in the later stages to match the specification.
  // ************************/

  // printf("|");
  // for (int i = 0; i < relCatEntry.numAttrs; ++i) {
  //   AttrCatEntry attrCatEntry;
  //   // get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
  //   AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);    

  //   printf(" %s |", attrCatEntry.attrName);
  // }
  // printf("\n");

  // while (true) {
  //   RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

  //   if (searchRes.block != -1 && searchRes.slot != -1) {
  //       RecBuffer recBlock(searchRes.block);
  //       HeadInfo head;
  //       recBlock.getHeader(&head);
        
  //       Attribute rec[head.numAttrs];
  //       recBlock.getRecord(rec,searchRes.slot);

  //     // get the record at searchRes using BlockBuffer.getRecord
        
  //     // print the attribute values in the same format as above
  //     printf("|");
  //     for(int i=0;i<head.numAttrs;i++)
  //     {
  //       AttrCatEntry temp;
  //       AttrCacheTable::getAttrCatEntry(srcRelId,i,&temp);
  //       if(temp.attrType==NUMBER)
  //       {
  //           printf(" %.0f |",rec[i].nVal);
  //       }
  //       else{
  //           printf(" %s |",rec[i].sVal);
  //       }
        
  //     }
  //     printf("\n");

  //   } else {

  //     // (all records over)
  //     break;
  //   }
  // }
 /*** Creating and opening the target relation ***/
    // Prepare arguments for createRel() in the following way:
    // get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);
    int src_nAttrs = relCatEntry.numAttrs/* the no. of attributes present in src relation */ ;


    /* let attr_names[src_nAttrs][ATTR_SIZE] be a 2D array of type char
        (will store the attribute names of rel). */
    // let attr_types[src_nAttrs] be an array of type int
    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];
    /*iterate through 0 to src_nAttrs-1 :
        get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
        fill the attr_names, attr_types arrays that we declared with the entries
        of corresponding attributes
    */
   for(int i=0;i<src_nAttrs;i++)
   {
     AttrCatEntry attrEntry;
     AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrEntry);
     strcpy(attr_names[i],attrEntry.attrName);
     attr_types[i]=attrEntry.attrType;
   }

    /* Create the relation for target relation by calling Schema::createRel()
       by providing appropriate arguments */
    // if the createRel returns an error code, then return that value.
   int retVal=Schema::createRel(targetRel,src_nAttrs,attr_names,attr_types);
   if(retVal!=SUCCESS)
   return retVal;
   int targetRelId=OpenRelTable::openRel(targetRel);
    /* Open the newly created target relation by calling OpenRelTable::openRel()
       method and store the target relid */
    /* If opening fails, delete the target relation by calling Schema::deleteRel()
       and return the error value returned from openRel() */
    if(targetRelId<0 || targetRelId>=MAX_OPEN)
    {
      Schema::deleteRel(targetRel);
      return targetRelId;
    }
    /*** Selecting and inserting records into the target relation ***/
    /* Before calling the search function, reset the search to start from the
       first using RelCacheTable::resetSearchIndex() */

    Attribute record[src_nAttrs];

    /*
        The BlockAccess::search() function can either do a linearSearch or
        a B+ tree search. Hence, reset the search index of the relation in the
        relation cache using RelCacheTable::resetSearchIndex().
        Also, reset the search index in the attribute cache for the select
        condition attribute with name given by the argument `attr`. Use
        AttrCacheTable::resetSearchIndex().
        Both these calls are necessary to ensure that search begins from the
        first record.
    */
    RelCacheTable::resetSearchIndex(srcRelId);
    // AttrCacheTable::resetSearchIndex(/* fill arguments */);

    // read every record that satisfies the condition by repeatedly calling
    // BlockAccess::search() until there are no more records to be read

    while (BlockAccess::search(srcRelId,record,attr,attrVal,op)==SUCCESS/* BlockAccess::search() returns success */) {

        // ret = BlockAccess::insert(targetRelId, record);
    int ret=BlockAccess::insert(targetRelId,record);
    if(ret!=SUCCESS)
    {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
        // if (insert fails) {
        //     close the targetrel(by calling Schema::closeRel(targetrel))
        //     delete targetrel (by calling Schema::deleteRel(targetrel))
        //     return ret;
        // }
    }

    // Close the targetRel by calling closeRel() method of schema layer
    Schema::closeRel(targetRel);
  return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    // if relName is equal to "RELATIONCAT" or "ATTRIBUTECAT"
    // return E_NOTPERMITTED;
    if(strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0)
    {
      return E_NOTPERMITTED;
    }

    // get the relation's rel-id using OpenRelTable::getRelId() method
    int relId = OpenRelTable::getRelId(relName);

    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)
    if(relId==E_RELNOTOPEN)
      return E_RELNOTOPEN;
    // get the relation catalog entry from relation cache
    // (use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId,&relCatEntry);
    /* if relCatEntry.numAttrs != numberOfAttributes in relation,
       return E_NATTRMISMATCH */
    if(relCatEntry.numAttrs!=nAttrs)
      return E_NATTRMISMATCH;
    // let recordValues[numberOfAttributes] be an array of type union Attribute
    Attribute recordValues[nAttrs];
    /*
        Converting 2D char array of record values to Attribute array recordValues
     */
    AttrCatEntry attrCatEntry;
    // iterate through 0 to nAttrs-1: (let i be the iterator)
    for(int i=0;i<nAttrs;i++)
    {
      AttrCacheTable::getAttrCatEntry(relId,i,&attrCatEntry);
        // get the attr-cat entry for the i'th attribute from the attr-cache
        // (use AttrCacheTable::getAttrCatEntry())

        // let type = attrCatEntry.attrType;
      int type=attrCatEntry.attrType;
        if (type == NUMBER)
        {
            // if the char array record[i] can be converted to a number
            // (check this using isNumber() function)
            
            if(isNumber(record[i]))
            {
                /* convert the char array to numeral and store it
                   at recordValues[i].nVal using atof() */
               recordValues[i].nVal=atof(record[i]);    
            }
            else
            {
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if (type == STRING)
        {
          strcpy(recordValues[i].sVal, record[i]);
            // copy record[i] to recordValues[i].sVal
        }
    }
    int retVal=BlockAccess::insert(relId,recordValues);
    // insert the record by calling BlockAccess::insert() function
    // let retVal denote the return value of insert call

    return retVal;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

    int srcRelId =OpenRelTable::getRelId(srcRel); /*srcRel's rel-id (use OpenRelTable::getRelId() function)*/
    if(srcRelId<0 || srcRelId>=MAX_OPEN)
     return srcRelId;
    // if srcRel is not open in open relation table, return E_RELNOTOPEN

    // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);
    // get the no. of attributes present in relation from the fetched RelCatEntry.
    int numAttrs=relCatEntry.numAttrs;
    // attrNames and attrTypes will be used to store the attribute names
    // and types of the source relation respectively
    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    /*iterate through every attribute of the source relation :
        - get the AttributeCat entry of the attribute with offset.
          (using AttrCacheTable::getAttrCatEntry())
        - fill the arrays `attrNames` and `attrTypes` that we declared earlier
          with the data about each attribute
    */
 for(int i=0;i<numAttrs;i++)
   {
     AttrCatEntry attrEntry;
     AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrEntry);
     strcpy(attrNames[i],attrEntry.attrName);
     attrTypes[i]=attrEntry.attrType;
   }

    /*** Creating and opening the target relation ***/

    // Create a relation for target relation by calling Schema::createRel()

    // if the createRel returns an error code, then return that value.
    int ret=Schema::createRel(targetRel,numAttrs,attrNames,attrTypes);
    if(ret!=SUCCESS)
     return ret;
    // Open the newly created target relation by calling OpenRelTable::openRel()
    // and get the target relid
    int targetRelId=OpenRelTable::openRel(targetRel);
    if(targetRelId<0 || targetRelId>=MAX_OPEN)
    {
      Schema::deleteRel(targetRel);
      return targetRelId;
    }
    // If opening fails, delete the target relation by calling Schema::deleteRel() of
    // return the error value returned from openRel().


    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[numAttrs];


    while (BlockAccess::project(srcRelId,record)==SUCCESS/* BlockAccess::project(srcRelId, record) returns SUCCESS */)
    {
        // record will contain the next record
 
        // ret = BlockAccess::insert(targetRelId, proj_record);
      ret=BlockAccess::insert(targetRelId,record);
        if (ret!=SUCCESS/* insert fails */) {
            // close the targetrel by calling Schema::closeRel()
            // delete targetrel by calling Schema::deleteRel()
            // return ret;
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Close the targetRel by calling Schema::closeRel()
    Schema::closeRel(targetRel);
    return SUCCESS;
    // return SUCCESS.
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {

    int srcRelId =OpenRelTable::getRelId(srcRel); /*srcRel's rel-id (use OpenRelTable::getRelId() function)*/
    if(srcRelId<0 || srcRelId>=MAX_OPEN)
      return srcRelId;
    // if srcRel is not open in open relation table, return E_RELNOTOPEN

    // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);
    // get the no. of attributes present in relation from the fetched RelCatEntry.
    int src_nAttrs=relCatEntry.numAttrs;
    // declare attr_offset[tar_nAttrs] an array of type int.
    // where i-th entry will store the offset in a record of srcRel for the
    // i-th attribute in the target relation.
    int attr_offset[tar_nAttrs];
    // let attr_types[tar_nAttrs] be an array of type int.
    // where i-th entry will store the type of the i-th attribute in the
    // target relation.
    int attr_types[tar_nAttrs];

    /*** Checking if attributes of target are present in the source relation
         and storing its offsets and types ***/

    /*iterate through 0 to tar_nAttrs-1 :
        - get the attribute catalog entry of the attribute with name tar_attrs[i].
        - if the attribute is not found return E_ATTRNOTEXIST
        - fill the attr_offset, attr_types arrays of target relation from the
          corresponding attribute catalog entries of source relation
    */
    for(int i=0;i<tar_nAttrs;i++)
    {
      AttrCatEntry attrEntry;
      int ret=AttrCacheTable::getAttrCatEntry(srcRelId,tar_Attrs[i],&attrEntry);
      if(ret!=SUCCESS)
      {
        return E_ATTRNOTEXIST;
      }
      attr_offset[i]=attrEntry.offset;
      attr_types[i]=attrEntry.attrType;
    }

    /*** Creating and opening the target relation ***/

    // Create a relation for target relation by calling Schema::createRel()
    int retVal=Schema::createRel(targetRel,tar_nAttrs,tar_Attrs,attr_types);
    // if the createRel returns an error code, then return that value.
    if(retVal!=SUCCESS)
    {
      return retVal;
    }
    // Open the newly created target relation by calling OpenRelTable::openRel()
    // and get the target relid

    // If opening fails, delete the target relation by calling Schema::deleteRel()
    // and return the error value from openRel()
    int targetRelId=OpenRelTable::openRel(targetRel);
    if(targetRelId<0 || targetRelId>=MAX_OPEN)
    {
      Schema::deleteRel(targetRel);
      return targetRelId;
    }

    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()

    Attribute record[src_nAttrs];

    while (BlockAccess::project(srcRelId,record)==SUCCESS/* BlockAccess::project(srcRelId, record) returns SUCCESS */) {
        // the variable `record` will contain the next record

        Attribute proj_record[tar_nAttrs];
         
        //iterate through 0 to tar_attrs-1:
        //    proj_record[attr_iter] = record[attr_offset[attr_iter]]
        for(int attr_iter=0;attr_iter<tar_nAttrs;attr_iter++)
        {
          proj_record[attr_iter]=record[attr_offset[attr_iter]];
        }
        // ret = BlockAccess::insert(targetRelId, proj_record);
        int ret=BlockAccess::insert(targetRelId,proj_record);
        if (ret!=SUCCESS/* insert fails */) {
            // close the targetrel by calling Schema::closeRel()
            // delete targetrel by calling Schema::deleteRel()
            // return ret;
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Close the targetRel by calling Schema::closeRel()
     Schema::closeRel(targetRel);
     return SUCCESS;
}
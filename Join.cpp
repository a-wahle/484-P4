#include "Join.hpp"

#include <vector>
#include <iostream>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	//for each page in relation
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));

	for(size_t i=left_rel.first; i<left_rel.second; ++i){
		mem->loadFromDisk(disk, i, 0);
		Page* input = mem->mem_page(0);
		//for each record on page
		for(size_t j=0; j<input->size(); ++j){
			Record record = input->get_record(j);
			auto hash_value = record.partition_hash() % (MEM_SIZE_IN_PAGE-1);
			Page* output = mem->mem_page(hash_value+1);
			output->loadRecord(record);
			if(output->full()){
				int disk_page_id = mem->flushToDisk(disk, hash_value+1);
				partitions[hash_value].add_left_rel_page(disk_page_id);
			}
		}
	}
	//Write all non-empty pages back to disk, because relations should not mix pages
	for(size_t i=1; i<MEM_SIZE_IN_PAGE; ++i){
		if(!mem->mem_page(i)->empty()){
			int disk_page_id = mem->flushToDisk(disk, i);
			partitions[i-1].add_left_rel_page(disk_page_id);
		}
	}

	for(size_t i=right_rel.first; i<right_rel.second; ++i){
		mem->loadFromDisk(disk, i, 0);
		Page* input = mem->mem_page(0);
		//for each record on page
		for(size_t j=0; j<input->size(); ++j){
			Record record = input->get_record(j);
			auto hash_value = record.partition_hash() % (MEM_SIZE_IN_PAGE-1);
			Page* output = mem->mem_page(hash_value+1);
			output->loadRecord(record);
			if(output->full()){
				int disk_page_id = mem->flushToDisk(disk, hash_value+1);
				partitions[hash_value].add_right_rel_page(disk_page_id);
			}
		}
	}
	//Write all non-empty pages back to disk, because relations should not mix pages
	for(size_t i=1; i<MEM_SIZE_IN_PAGE; ++i){
		if(!mem->mem_page(i)->empty()){
			int disk_page_id = mem->flushToDisk(disk, i);
			partitions[i-1].add_right_rel_page(disk_page_id);
		}
	}
	
	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages; // placeholder
	return disk_pages;
}

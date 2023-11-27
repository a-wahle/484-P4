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
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE-1, Bucket(disk));

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

void create_mem_table(Disk* disk, Mem* mem, vector<uint> b){
	for(int i: b){
		mem->loadFromDisk(disk, i, 0);
		Page* input = mem->mem_page(0);
		for(size_t j=0; j<input->size(); ++j){
			Record record = input->get_record(j);
			auto hash_value = record.probe_hash() % (MEM_SIZE_IN_PAGE-2);
			Page* output = mem->mem_page(hash_value+1);
			output->loadRecord(record);
		}
	}
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages;
	int left_size = 0;
	int right_size = 0;
	for(auto p:partitions){
		left_size += p.num_left_rel_record;
		right_size += p.num_right_rel_record;
	}

	//if(left_size<right_size){
		Page* join_page = mem->mem_page(MEM_SIZE_IN_PAGE-1);
		for(auto b: partitions){
			vector<uint> larger_relation;
			if(left_size < right_size){
				create_mem_table(disk, mem, b.get_left_rel());
				larger_relation = b.get_right_rel();
			}
			else{
				create_mem_table(disk, mem, b.get_right_rel());
				larger_relation = b.get_left_rel();
			}
			
			for(int i: larger_relation){
				mem->loadFromDisk(disk, i, 0);
				Page* input = mem->mem_page(0);
				for(size_t j=0; j<input->size(); ++j){
					Record right_record = input->get_record(j);
					auto right_hash_value = right_record.probe_hash() % (MEM_SIZE_IN_PAGE-2);
					Page* left_page = mem->mem_page(right_hash_value+1);
					for(size_t k =0; k<left_page->size(); ++k){
						Record left_record = left_page->get_record(k);
						if(left_record == right_record){
							join_page->loadPair(left_record, right_record);
						}
						if(join_page->full()){
							int disk_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE-1);
							disk_pages.push_back(disk_id);
						}
					}
					
				}
			}
			for (size_t page_id = 1; page_id <= MEM_SIZE_IN_PAGE - 2; ++page_id) {
				Page* temp = mem->mem_page(page_id);
				temp->reset();
			}
		} 
		disk_pages.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE-1));
	//}

	return disk_pages;
}

#!/usr/bin/env python3
"""
Firestore LevelDB Export to JSON Converter

This script reads a Firestore database export in LevelDB format and converts
all documents to JSON format.
"""

import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
from leveldb_export import parse_leveldb_documents

def find_export_files(export_dir: str) -> List[str]:
    """Find all output files in the export directory."""
    export_path = Path(export_dir)
    if not export_path.exists():
        raise FileNotFoundError(f"Export directory not found: {export_dir}")
    
    # Find all output-* files
    output_files = []
    for file_path in export_path.glob("output-*"):
        if file_path.is_file():
            output_files.append(str(file_path))
    
    # Sort files numerically (output-0, output-1, ..., output-241)
    output_files.sort(key=lambda x: int(Path(x).name.split('-')[1]))
    
    return output_files

def parse_single_file(file_path: str) -> List[Dict[str, Any]]:
    """Parse documents from a single LevelDB file."""
    try:
        docs = list(parse_leveldb_documents(file_path))
        print(f"Parsed {len(docs)} documents from {Path(file_path).name}")
        return docs
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
        return []

def extract_collection_info(doc: Dict[str, Any]) -> tuple:
    """Extract collection name and document ID from document."""
    if '_key' not in doc:
        return None, None
    
    key_data = doc['_key']
    if not isinstance(key_data, dict) or 'path' not in key_data:
        return None, None
    
    path = key_data['path']
    if not isinstance(path, str):
        return None, None
    
    # Split path to get collection and document ID
    path_parts = path.split('/')
    if len(path_parts) >= 2:
        collection_name = path_parts[0]
        document_id = path_parts[1]
        return collection_name, document_id
    
    return None, None

def convert_firestore_to_json(export_dir: str, output_file: str = "firestore_export.json") -> None:
    """
    Convert Firestore LevelDB export to JSON format.
    
    Args:
        export_dir: Path to the directory containing the LevelDB export files
        output_file: Name of the output JSON file
    """
    print(f"Starting conversion of Firestore export from: {export_dir}")
    
    # Find all export files
    export_files = find_export_files(export_dir)
    print(f"Found {len(export_files)} export files")
    
    if not export_files:
        print("No export files found. Make sure the export directory contains output-* files.")
        return
    
    # Parse all files and collect documents
    all_documents = []
    total_docs = 0
    
    for file_path in export_files:
        docs = parse_single_file(file_path)
        all_documents.extend(docs)
        total_docs += len(docs)
    
    print(f"Total documents parsed: {total_docs}")
    
    # Organize documents by collection
    collections = {}
    orphaned_docs = []
    
    for doc in all_documents:
        collection_name, document_id = extract_collection_info(doc)
        
        if collection_name:
            if collection_name not in collections:
                collections[collection_name] = []
            
            # Add document ID to the document for easier identification
            doc_with_id = doc.copy()
            doc_with_id['_document_id'] = document_id
            collections[collection_name].append(doc_with_id)
        else:
            orphaned_docs.append(doc)
    
    # Create the final JSON structure
    result = {
        "metadata": {
            "export_date": None,  # Will be filled if available from metadata
            "total_documents": total_docs,
            "total_collections": len(collections),
            "export_files_processed": len(export_files)
        },
        "collections": collections
    }
    
    # Add orphaned documents if any
    if orphaned_docs:
        result["orphaned_documents"] = orphaned_docs
        print(f"Found {len(orphaned_docs)} orphaned documents (without clear collection path)")
    
    # Try to read metadata if available
    metadata_file = Path(export_dir) / "all_namespaces_all_kinds.export_metadata"
    if metadata_file.exists():
        try:
            # Try to read as binary first, then as text
            with open(metadata_file, 'rb') as f:
                metadata_content = f.read()
                # Try to decode as UTF-8, if that fails, use latin-1
                try:
                    metadata_str = metadata_content.decode('utf-8')
                except UnicodeDecodeError:
                    metadata_str = metadata_content.decode('latin-1')
                result["metadata"]["export_metadata"] = metadata_str
                print(f"Added export metadata from {metadata_file.name}")
        except Exception as e:
            print(f"Could not read metadata file: {e}")
    
    # Write the result to JSON file
    print(f"Writing results to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"Conversion complete!")
    print(f"Output file: {output_file}")
    print(f"Collections found: {list(collections.keys())}")
    print(f"Documents per collection:")
    for collection, docs in collections.items():
        print(f"  {collection}: {len(docs)} documents")

def main():
    """Main function to run the conversion."""
    if len(sys.argv) < 2:
        print("Usage: python firestore_to_json.py <export_directory> [output_file]")
        print("Example: python firestore_to_json.py ./backups/all_namespaces/all_kinds")
        sys.exit(1)
    
    export_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "firestore_export.json"
    
    try:
        convert_firestore_to_json(export_dir, output_file)
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
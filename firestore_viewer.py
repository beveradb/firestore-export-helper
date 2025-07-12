#!/usr/bin/env python3
"""
Firestore JSON Viewer

A simple interactive viewer for exploring the converted Firestore JSON data.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any

class FirestoreViewer:
    def __init__(self, json_file: str):
        self.json_file = json_file
        self.data = None
        self.load_data()
    
    def load_data(self):
        """Load the JSON data from file."""
        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            print(f"Successfully loaded data from {self.json_file}")
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            sys.exit(1)
    
    def show_summary(self):
        """Display a summary of the data."""
        print("\n" + "="*50)
        print("FIRESTORE EXPORT SUMMARY")
        print("="*50)
        
        metadata = self.data.get('metadata', {})
        print(f"Total documents: {metadata.get('total_documents', 'Unknown')}")
        print(f"Total collections: {metadata.get('total_collections', 'Unknown')}")
        print(f"Export files processed: {metadata.get('export_files_processed', 'Unknown')}")
        
        collections = self.data.get('collections', {})
        print(f"\nCollections found:")
        for collection_name, docs in collections.items():
            print(f"  {collection_name}: {len(docs)} documents")
        
        orphaned = self.data.get('orphaned_documents', [])
        if orphaned:
            print(f"\nOrphaned documents: {len(orphaned)}")
    
    def show_collection_details(self, collection_name: str):
        """Show details of a specific collection."""
        collections = self.data.get('collections', {})
        if collection_name not in collections:
            print(f"Collection '{collection_name}' not found.")
            return
        
        docs = collections[collection_name]
        print(f"\n" + "="*50)
        print(f"COLLECTION: {collection_name}")
        print("="*50)
        print(f"Total documents: {len(docs)}")
        
        # Show sample document structure
        if docs:
            print(f"\nSample document structure:")
            sample_doc = docs[0]
            for key, value in sample_doc.items():
                if key.startswith('_'):
                    continue  # Skip internal fields
                value_type = type(value).__name__
                if isinstance(value, str) and len(value) > 50:
                    display_value = value[:50] + "..."
                elif isinstance(value, dict):
                    display_value = f"{{dict with {len(value)} keys}}"
                elif isinstance(value, list):
                    display_value = f"[list with {len(value)} items]"
                else:
                    display_value = str(value)
                print(f"  {key}: {display_value} ({value_type})")
    
    def show_document(self, collection_name: str, doc_index: int):
        """Show a specific document."""
        collections = self.data.get('collections', {})
        if collection_name not in collections:
            print(f"Collection '{collection_name}' not found.")
            return
        
        docs = collections[collection_name]
        if doc_index >= len(docs):
            print(f"Document index {doc_index} out of range. Collection has {len(docs)} documents.")
            return
        
        doc = docs[doc_index]
        print(f"\n" + "="*50)
        print(f"DOCUMENT: {collection_name}[{doc_index}]")
        print("="*50)
        
        # Pretty print the document
        print(json.dumps(doc, indent=2, ensure_ascii=False, default=str))
    
    def search_documents(self, collection_name: str, field: str, value: str):
        """Search for documents with a specific field value."""
        collections = self.data.get('collections', {})
        if collection_name not in collections:
            print(f"Collection '{collection_name}' not found.")
            return
        
        docs = collections[collection_name]
        matches = []
        
        for i, doc in enumerate(docs):
            if field in doc:
                doc_value = str(doc[field]).lower()
                if value.lower() in doc_value:
                    matches.append((i, doc))
        
        print(f"\n" + "="*50)
        print(f"SEARCH RESULTS: {collection_name}.{field} contains '{value}'")
        print("="*50)
        print(f"Found {len(matches)} matches:")
        
        for i, (doc_index, doc) in enumerate(matches[:10]):  # Show first 10 matches
            print(f"\n[{i+1}] Document {doc_index}:")
            print(f"  {field}: {doc[field]}")
            # Show a few other fields for context
            for key, val in list(doc.items())[:3]:
                if key != field and not key.startswith('_'):
                    display_val = str(val)[:100] + "..." if len(str(val)) > 100 else str(val)
                    print(f"  {key}: {display_val}")
        
        if len(matches) > 10:
            print(f"\n... and {len(matches) - 10} more matches")
    
    def interactive_mode(self):
        """Start interactive exploration mode."""
        print("\n" + "="*50)
        print("INTERACTIVE MODE")
        print("="*50)
        print("Commands:")
        print("  summary - Show data summary")
        print("  list - List all collections")
        print("  show <collection> - Show collection details")
        print("  doc <collection> <index> - Show specific document")
        print("  search <collection> <field> <value> - Search documents")
        print("  quit - Exit")
        print("="*50)
        
        while True:
            try:
                command = input("\n> ").strip().split()
                if not command:
                    continue
                
                if command[0] == 'quit':
                    break
                elif command[0] == 'summary':
                    self.show_summary()
                elif command[0] == 'list':
                    collections = self.data.get('collections', {})
                    print("\nAvailable collections:")
                    for name in collections.keys():
                        print(f"  {name}")
                elif command[0] == 'show' and len(command) > 1:
                    self.show_collection_details(command[1])
                elif command[0] == 'doc' and len(command) > 2:
                    try:
                        self.show_document(command[1], int(command[2]))
                    except ValueError:
                        print("Invalid document index. Must be a number.")
                elif command[0] == 'search' and len(command) > 3:
                    self.search_documents(command[1], command[2], ' '.join(command[3:]))
                else:
                    print("Invalid command. Type 'quit' to exit.")
            
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {e}")

def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python firestore_viewer.py <json_file>")
        print("Example: python firestore_viewer.py firestore_export_fixed.json")
        sys.exit(1)
    
    json_file = sys.argv[1]
    if not Path(json_file).exists():
        print(f"JSON file not found: {json_file}")
        sys.exit(1)
    
    viewer = FirestoreViewer(json_file)
    viewer.show_summary()
    viewer.interactive_mode()

if __name__ == "__main__":
    main() 
import faiss
import numpy as np

class FaissIndex:
    def __init__(self, vector_dim=1024):
        self.vector_dim = vector_dim
        self.index = faiss.IndexFlatL2(vector_dim)
    
    def add_vector(self, vector):
        self.index.add(vector)
    
    def search(self, vector, k=1):
        return self.index.search(vector, k)
    
    def clear(self):
        """Clear the FAISS index."""
        self.index = faiss.IndexFlatL2(self.vector_dim)
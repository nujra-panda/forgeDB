#include <iostream>
#include <vector>
#include <algorithm>
#include <queue>

// --- CONFIGURATION ---
const int MAX_KEYS = 3; // Small size to force splits

// --- NODE STRUCTURES ---
struct Node {
    bool is_leaf;
    std::vector<int> keys;
    Node* parent;

    Node(bool leaf) : is_leaf(leaf), parent(nullptr) {}
    virtual ~Node() {}
};

struct InternalNode : Node {
    std::vector<Node*> children;
    InternalNode() : Node(false) {}
};

struct LeafNode : Node {
    LeafNode* next;
    LeafNode() : Node(true), next(nullptr) {}
};

// --- B+ TREE CLASS ---
class BPlusTree {
public:
    Node* root;

    BPlusTree() {
        root = new LeafNode();
    }

    // --- HELPER: FIND LEAF ---
    LeafNode* find_leaf(int key) {
        Node* curr = root;
        while (!curr->is_leaf) {
            InternalNode* internal = static_cast<InternalNode*>(curr);
            size_t i = 0;
            while (i < internal->keys.size() && key >= internal->keys[i]) {
                i++;
            }
            curr = internal->children[i];
        }
        return static_cast<LeafNode*>(curr);
    }

    // --- INSERTION ---
    void insert(int key) {
        LeafNode* leaf = find_leaf(key);
        leaf->keys.push_back(key);
        std::sort(leaf->keys.begin(), leaf->keys.end());

        if (leaf->keys.size() > MAX_KEYS) {
            split_leaf(leaf);
        }
    }

    // --- LEAF SPLIT  ---
    void split_leaf(LeafNode* left) {
        LeafNode* right = new LeafNode();
        int split_index = (left->keys.size() + 1) / 2;

        for (size_t i = split_index; i < left->keys.size(); i++) right->keys.push_back(left->keys[i]);
        left->keys.resize(split_index);

        right->next = left->next;
        left->next = right;

        int promote_key = right->keys[0]; // Copy the key
        insert_into_parent(left, promote_key, right);
    }

    // --- INTERNAL SPLIT (PUSH UP) ---
    void split_internal(InternalNode* left) {
        InternalNode* right = new InternalNode();
        
        // 1. Determine Split Point
        // If keys are [3, 5, 7, 9], size is 4. Mid is index 2 (Key 7).
        int mid_index = left->keys.size() / 2;
        int promote_key = left->keys[mid_index]; // This key goes UP

        // 2. Move Keys to Right Node (Strictly AFTER mid)
        // Right gets [9]
        for (size_t i = mid_index + 1; i < left->keys.size(); i++) {
            right->keys.push_back(left->keys[i]);
        }

        // 3. Move Children to Right Node
        // An internal node with N keys has N+1 children.
        // We move the second half of children to the new node.
        for (size_t i = mid_index + 1; i < left->children.size(); i++) {
            right->children.push_back(left->children[i]);
            left->children[i]->parent = right; // Update child's parent ptr
        }

        // 4. Resize Left Node
        // Left keeps [3, 5]. promote_key (7) is removed from both!
        left->keys.resize(mid_index);
        left->children.resize(mid_index + 1);

        // 5. Recursive Push
        insert_into_parent(left, promote_key, right);
    }

    void insert_into_parent(Node* left, int key, Node* right) {
        // Case 1: Root Split
        if (left->parent == nullptr) {
            InternalNode* new_root = new InternalNode();
            new_root->keys.push_back(key);
            new_root->children.push_back(left);
            new_root->children.push_back(right);
            left->parent = new_root;
            right->parent = new_root;
            root = new_root;
            std::cout << "DEBUG: Root Split! New Root Key: " << key << "\n";
            return;
        }

        // Case 2: Generic Parent Insert
        InternalNode* parent = static_cast<InternalNode*>(left->parent);
        size_t insert_pos = 0;
        while (insert_pos < parent->keys.size() && parent->keys[insert_pos] < key) insert_pos++;

        parent->keys.insert(parent->keys.begin() + insert_pos, key);
        parent->children.insert(parent->children.begin() + insert_pos + 1, right);
        right->parent = parent;

        // Case 3: Recursion
        if (parent->keys.size() > MAX_KEYS) {
            std::cout << "DEBUG: Internal Node Overflow! Splitting...\n";
            split_internal(parent);
        }
    }

    // --- VISUALIZATION ---
    void print() {
        if (!root) return;
        std::cout << "\n--- Tree Structure ---\n";
        std::queue<Node*> q;
        q.push(root);
        while (!q.empty()) {
            int level_size = q.size();
            while (level_size > 0) {
                Node* current = q.front();
                q.pop();

                std::cout << "[";
                for (size_t i = 0; i < current->keys.size(); i++) {
                    std::cout << current->keys[i];
                    if (i < current->keys.size() - 1) std::cout << "|";
                }
                std::cout << "] ";

                if (!current->is_leaf) {
                    InternalNode* internal = static_cast<InternalNode*>(current);
                    for (Node* child : internal->children) q.push(child);
                }
                level_size--;
            }
            std::cout << "\n";
        }
        std::cout << "----------------------\n";
    }
};

int main() {
    BPlusTree tree;

    // 1. Fill leaf (1, 2, 3)
    // 2. Split Leaf -> Root [3], Leaves [1,2], [3,4]
    // 3. Keep inserting to fill the Root Node
    // Root Max Keys = 3. We need 4 keys in Root to force it to split.
    // Each key in root comes from a Leaf Split.
    // So we need about 12-13 inserts to trigger a height increase.
    
    std::cout << "--- Inserting 1 to 15 ---\n";
    for (int i = 1; i <= 15; i++) {
        std::cout << "Inserting " << i << "...\n";
        tree.insert(i);
        // tree.print(); // Uncomment to see every step
    }
    
    tree.print();
    return 0;
}
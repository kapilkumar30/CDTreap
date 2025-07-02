/* Concurrent implementation of Treaps*/
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.Stack;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
public class ConFatTreap_SHA256 {
	
	public static int FATSIZE;
	//public static int SHIFT;
	public static int NUM_THREADS;
	class Node{
		final int key; // Value of a node
		final int pri; // Priority of a node
		public ConcurrentHashMap <Integer,Integer>instance;
		volatile boolean mark; // whether a node is deleted or not
		volatile boolean Transient_State1, Transient_State2;
		volatile Node left, right, parent; // left- left subtree ref. right - right subtree ref. parent - parent node ref.
		volatile Node pred, succ; // pred and succ are for predecessor and successor of a node.
		//volatile ReentrantLock Lock_Node;
		public ReentrantReadWriteLock RWLock_Node;

		public Node(int data1) {
			int size=FATSIZE;
			instance=new ConcurrentHashMap <Integer,Integer>(size,0.75f,NUM_THREADS);
			instance.put(data1,data1);
			this.key=data1/FATSIZE;
			this.pri = sha256(this.key);
			this.mark = false;
			this.Transient_State1 = true;
			this.Transient_State2 = false;
			this.left = null;
			this.right = null;
			this.parent = null;
			this.pred = null;
			this.succ = null;
			//this.Lock_Node = new ReentrantLock();
			this.RWLock_Node = new ReentrantReadWriteLock();
		}

		public Node(int data1, int p) {
			this.key = data1;
			this.pri = p;
			this.mark = false;
			this.Transient_State1 = true;
			this.Transient_State2 = false;
			this.left = null;
			this.right = null;
			this.parent = null;
			this.pred = null;
			this.succ = null;
			//this.Lock_Node = new ReentrantLock();
			this.RWLock_Node = new ReentrantReadWriteLock();
		}


		private int sha256(int data) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			byte[] bytes = ByteBuffer.allocate(4).putInt(data).array();
			byte[] hashBytes = md.digest(bytes);
			int hash = 0;
			for (byte b : hashBytes) {
				hash = (hash << 8) | (b & 0xFF);
			}
			return hash;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	}

	Random rand;
	int count=0;
	public Node root=null;
	

	public ConFatTreap_SHA256(int num_threads, int fat_node_size) {
		FATSIZE=fat_node_size;
		
		NUM_THREADS=num_threads;
		root = new Node(Integer.MAX_VALUE, Integer.MAX_VALUE);
		root.left = new Node(Integer.MAX_VALUE - 1, Integer.MAX_VALUE - 1);
		root.left.left = new Node(Integer.MIN_VALUE, Integer.MIN_VALUE);
		root.left.parent = root;
		root.pred = root.left;
		root.left.succ = root;
		root.left.pred = root.left.left;
		root.left.left.succ = root.left;
		root.left.left.parent = root.left;
		root.Transient_State1 = false;
		root.left.Transient_State1 = false;
		root.left.left.Transient_State1 = false;
	}
	public boolean insert(int x)
	{
		Node node, pred, succ, newNode, parent;
			int k;
		while(true)
		{
			//Node node,p,s,parent,newNode;// p is predecessor of a node, s is successor of a node. newNode is newly inserted node.
			node=search(x);// If treap contains a node with value x then return that. Otherwise return the terminated node.
			k=x/FATSIZE;
			if(node.key==k && node.Transient_State1==false && node.Transient_State2==false && node.mark==false)
				{
			if(!node.RWLock_Node.readLock().tryLock()){continue;}
			   Integer val1=node.instance.putIfAbsent(x,x);
				node.RWLock_Node.readLock().unlock();
				if(val1==null){ return true;} 
				else{return false;}
				}

			parent=node.parent;
			pred=(node.key>=k)?node.pred:node;
			if(!pred.RWLock_Node.writeLock().tryLock())
			{
				continue; 
			}
			if(pred.Transient_State2){ pred.RWLock_Node.writeLock().unlock(); continue; }
			succ=pred.succ;
			if(succ.mark){ pred.RWLock_Node.writeLock().unlock(); continue;  }
			if(pred!=node&&!succ.RWLock_Node.writeLock().tryLock())
				{
					pred.RWLock_Node.writeLock().unlock();
					continue; 
				}
			if((k>pred.key)&&(k<=succ.key))
			{
				if(k==succ.key)
				{
					if(succ.Transient_State2){
						if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
						pred.RWLock_Node.writeLock().unlock(); continue;
					}
					else{
						Integer val1=succ.instance.putIfAbsent(x,x);
						if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
						pred.RWLock_Node.writeLock().unlock(); 
						if(val1==null){ return true;} 
						return false;
					}
				}
				//if(!s.trySuccLock()){ p.succUnlock(); continue; }
				
				if(node.Transient_State1||node.Transient_State2||parent!=node.parent)
				{
					pred.RWLock_Node.writeLock().unlock();
					if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
					continue;
				}
				else if(((k<node.key)&&(node.left==null))||((k>node.key)&&(node.right==null)))
				{	
						newNode=new Node(x);
						newNode.parent=node;
						newNode.succ=succ;
						newNode.pred=pred;

						succ.pred=newNode;
						pred.succ=newNode;
						
						if(k<node.key)
						{
							node.left=newNode;
						}
						else
						{
							node.right=newNode;
						}
						pred.RWLock_Node.writeLock().unlock();
						if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
						balanceTreap(newNode);
						return true;
				}
				else
				{
					pred.RWLock_Node.writeLock().unlock();
					if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
					continue;
				}
			}
			if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
			pred.RWLock_Node.writeLock().unlock();
		}
	}

// Newly inserted node is moved up, until node.parent priority is greater than node.priority. 
public void balanceTreap(Node node)
{
	while(true)
	{
			Node nParentParent=node.parent.parent;
			if(!nParentParent.RWLock_Node.writeLock().tryLock()){ 
				continue;
			 }
			if(nParentParent.mark)
			{  
				nParentParent.RWLock_Node.writeLock().unlock();
				continue; 
			}
			Node nParent=node.parent;
			if(!nParent.RWLock_Node.writeLock().tryLock()){ 
				nParentParent.RWLock_Node.writeLock().unlock();
				continue;
			}
			if(nParent.Transient_State1||nParent.Transient_State2)
			{
				nParentParent.RWLock_Node.writeLock().unlock();  nParent.RWLock_Node.writeLock().unlock();
				continue;
			}
			if(!node.RWLock_Node.writeLock().tryLock()){ nParentParent.RWLock_Node.writeLock().unlock();nParent.RWLock_Node.writeLock().unlock();
				continue; 
			}	
			if((nParentParent!=node.parent.parent)||(nParent!=node.parent)){
				nParentParent.RWLock_Node.writeLock().unlock();
				nParent.RWLock_Node.writeLock().unlock();
				node.RWLock_Node.writeLock().unlock();
				continue;
			}
			if(nParent.pri<node.pri)
			{
				boolean flag=false;
					if(nParent.left==node)
					{
						if(node.right!=null)
						{
							Node nRight=node.right;
							Node nRightParent=node.right.parent;
							if(!nRight.RWLock_Node.writeLock().tryLock())
							{ 
								 nParentParent.RWLock_Node.writeLock().unlock();
                               					 nParent.RWLock_Node.writeLock().unlock();
                                				 node.RWLock_Node.writeLock().unlock();
                                				 continue;

							}
							if((nRight!=node.right)||(nRight.mark)||nRightParent!=node)
							{
								nParentParent.RWLock_Node.writeLock().unlock();
				                                nParent.RWLock_Node.writeLock().unlock();
                                				node.RWLock_Node.writeLock().unlock();
								nRight.RWLock_Node.writeLock().unlock();;
								continue;
							}
							flag=true;
						}
			 			singleRotateRight(nParent, node);
						if(flag==true){ nParent.left.RWLock_Node.writeLock().unlock();  }
						nParent.RWLock_Node.writeLock().unlock();
										
					}
					else
					{
						if(node.left!=null)
						{
							Node nLeft=node.left,nLeftParent=node.left.parent;
							if(!nLeft.RWLock_Node.writeLock().tryLock())
							{
								 nParentParent.RWLock_Node.writeLock().unlock();
				                                 nParent.RWLock_Node.writeLock().unlock();
                                				 node.RWLock_Node.writeLock().unlock();
                                 				 continue;

							}
							if((nLeft!=node.left)||(nLeft.mark)||nLeftParent!=node)
							{
								nParentParent.RWLock_Node.writeLock().unlock();
                                				nParent.RWLock_Node.writeLock().unlock();
                                				node.RWLock_Node.writeLock().unlock();
								nLeft.RWLock_Node.writeLock().unlock();
								continue;
							}
							flag=true;
						}
						singleRotateLeft(nParent,node);
						if(flag==true){ nParent.right.RWLock_Node.writeLock().unlock();  }
						nParent.RWLock_Node.writeLock().unlock();
					}
					nParentParent.RWLock_Node.writeLock().unlock();
					node.RWLock_Node.writeLock().unlock();
			}
			else
			{
				node.Transient_State1=false;
				
				nParentParent.RWLock_Node.writeLock().unlock();
                                nParent.RWLock_Node.writeLock().unlock();
                                node.RWLock_Node.writeLock().unlock();
				break;
			}
		}
}

public void singleRotateRight(Node parent, Node node)
{
        Node temp=parent.parent;
        if(temp.left==parent)
        {
                temp.left=node;
        }
        else{
                temp.right=node;
        }
        node.parent=temp;
        Node nodeRight=node.right;
        node.right=parent;
        parent.left=nodeRight;
        if(nodeRight!=null)
        {
                nodeRight.parent=parent;
        }
        parent.parent=node;
        
}
// Rotate Left.
public void singleRotateLeft(Node parent, Node node)
{
        Node temp=parent.parent;
                if(temp.left==parent)//parent.parent left child is parent
                {
                        temp.left=node;
                }
                else{//parent.parent right child is  parent
                        temp.right=node;
                }
        node.parent=temp;
        Node nodeLeft=node.left;
        node.left=parent;
        parent.right=nodeLeft;
        if(nodeLeft!=null)
        {
                nodeLeft.parent=parent;
        }
        parent.parent=node;
}
// Search for a node, which has key x.
public Node search(int val)
	{

	int x=val/FATSIZE;
        Node T, child;
        T=root;
	
        while(T!=null)
        {
                int key=T.key;
                if(key==x) return T;
                if(key>x){
                        child=T.left;
                }
                else{
                        child=T.right;
                }
                if(child!=null){ T=child; }
                else{ return T; }

        }
                return T;		
	}

public boolean contains(int val)
	{
		Node node=search(val);
		int k=val/FATSIZE;
		while(node.key>k){ node=node.pred; }
		while(node.key<k){ node=node.succ; }
		if(node.key==k)
		{
				return node.instance.containsKey(val);
			
		}	
		return false;
	}

boolean remove(int x)
{
	while(true)
	{
		Node node=search(x);
		int k=x/FATSIZE;
		if(node.key==k && node.Transient_State1==false && node.Transient_State2==false && node.mark==false)
		{
			if(!node.RWLock_Node.readLock().tryLock()){continue;}
			   if(node.instance.size()>0){boolean flag=node.instance.remove(x,x);
			   	node.RWLock_Node.readLock().unlock(); 
				return flag;}else{node.RWLock_Node.readLock().unlock();}
				
		}

		Node pred=(node.key>=k)?node.pred:node;
		if(!pred.RWLock_Node.writeLock().tryLock()){  continue; }
		if(pred.mark){ pred.RWLock_Node.writeLock().unlock(); continue; }
		Node succ=pred.succ;
		if(pred!=node&&!succ.RWLock_Node.writeLock().tryLock())
				{
					pred.RWLock_Node.writeLock().unlock();
					continue; 
				}

		if((k>pred.key)&&(k<=succ.key))
		{
			if(k<succ.key){
				if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
				pred.RWLock_Node.writeLock().unlock();
				return false;
			}
			if(succ.instance.size()>0){
					boolean flag=succ.instance.remove(x,x);
					if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
                                        pred.RWLock_Node.writeLock().unlock();
                                        return flag;
				}
			//if(!succ.RWLock_Node.writeLock().tryLock()){ pred.RWLock_Node.writeLock().unlock(); continue; }
			if(succ.mark){pred.RWLock_Node.writeLock().unlock(); succ.RWLock_Node.writeLock().unlock(); return false; }
			if(succ.Transient_State1){ pred.RWLock_Node.writeLock().unlock(); succ.RWLock_Node.writeLock().unlock(); continue; }
			Node sSucc=succ.succ;
			if(sSucc.mark){ pred.RWLock_Node.writeLock().unlock(); succ.RWLock_Node.writeLock().unlock(); continue; }
			succ.Transient_State2=true;
			
			Node n=succ;
			boolean  flag=gainTreapLocks(n,pred);
			while(flag){
			if(n.left!=null&&n.right!=null){
				//succ.Transient_State2=true;
				Node  nChild=null;
				Node child=(n.left.pri>n.right.pri)?n.left:n.right;
				if((child==n.left)&&(child.right!=null)){ nChild=child.right; }
				else if((child==n.right)&&(child.left!=null)){ nChild=child.left; } 
				if(child==n.left){singleRotateRight(n,n.left); }
				else{ singleRotateLeft(n,n.right); }
				if(nChild!=null&&nChild!=pred){ nChild.RWLock_Node.writeLock().unlock(); }
				if(n.parent!=pred){ n.parent.RWLock_Node.writeLock().unlock(); }
				if(n.parent.parent!=pred){ n.parent.parent.RWLock_Node.writeLock().unlock(); }
				//n.parent.RWLock_Node.writeLock().unlock();
				flag=gainTreapLocks(n,pred);
			}
			else{
				//n.Transient_State2=true;
				n.mark=true;
				
			//	Node sSucc=succ.succ;
				sSucc.pred=pred;
				pred.succ=sSucc;
				Node child=(n.left!=null)?n.left:n.right;
				updateChild(n.parent,n,child);
				if((child!=null&&child!=pred)&&(n.parent!=pred)||(child==null&&n.parent!=pred)){pred.RWLock_Node.writeLock().unlock(); }
				if(child!=null){  child.RWLock_Node.writeLock().unlock();  }
				n.parent.RWLock_Node.writeLock().unlock(); 
				n.RWLock_Node.writeLock().unlock();
				//removeFromTree(n);
				return true;	
			}
			}
			if(!flag){ pred.RWLock_Node.writeLock().unlock(); succ.RWLock_Node.writeLock().unlock(); continue; }
		}
		if(pred!=node) { succ.RWLock_Node.writeLock().unlock(); }
		pred.RWLock_Node.writeLock().unlock();

	}
}

boolean gainTreapLocks(Node n,Node p)
{
	int i=0;
	while(true&&i<100)
	{
		i=i+1;
		Node nParent=n.parent;
			if(nParent!=p&&!nParent.RWLock_Node.writeLock().tryLock()){ continue; }
			if(nParent.mark||nParent!=n.parent){ if(nParent!=p){ nParent.RWLock_Node.writeLock().unlock();}/*System.out.println("Hello");*/ continue; }
	
		if(n.left==null||n.right==null)
		{
			Node child=null;
			if(n.right!=null)
			{
				child=n.right; 
			}
			if(n.left!=null)
			{
				child=n.left;
			}
			if(child!=null)
			{
				if(child!=p&&!child.RWLock_Node.writeLock().tryLock()){ if(nParent!=p){ nParent.RWLock_Node.writeLock().unlock();}  continue; }
				if((child.mark)||(child.parent!=n)){  if(nParent!=p){ nParent.RWLock_Node.writeLock().unlock();} if(child!=p){ child.RWLock_Node.writeLock().unlock();} continue; }
			}
			return true;
		}
		else{
			if(n.left.pri>n.right.pri){
				Node child1=n.left; 
				Node parent=child1.parent;
					if(child1!=p&&!child1.RWLock_Node.writeLock().tryLock()){  if(nParent!=p){ nParent.RWLock_Node.writeLock().unlock();} continue; }
					if((child1!=n.left)||(child1.mark)||(parent!=child1.parent)){ 
						if(nParent!=p){ nParent.RWLock_Node.writeLock().unlock();}
						if(child1!=p){ child1.RWLock_Node.writeLock().unlock();}
						continue; 
					}
				if(child1.right!=null){
					Node child1_Right=child1.right; 
					Node R_parent=child1_Right.parent;
						if(child1_Right!=p&&!child1_Right.RWLock_Node.writeLock().tryLock()){
							if(nParent!=p){nParent.RWLock_Node.writeLock().unlock();}
							if(child1!=p){child1.RWLock_Node.writeLock().unlock();}
							continue;
						}
						if((child1_Right!=child1.right)||(child1_Right.mark)||(R_parent!=child1_Right.parent)){
							if(nParent!=p){nParent.RWLock_Node.writeLock().unlock();}
							if(child1_Right!=p){ child1_Right.RWLock_Node.writeLock().unlock();}
							if(child1!=p){child1.RWLock_Node.writeLock().unlock();}
							continue; 
						}
					
				}
			}
			else{
				Node child1=n.right;
				Node parent=child1.parent;
					if(child1!=p&&!child1.RWLock_Node.writeLock().tryLock()){ nParent.RWLock_Node.writeLock().unlock();  continue; }
					if((child1!=n.right)||(child1.mark)||(parent!=child1.parent)){ 
						 if(nParent!=p){ nParent.RWLock_Node.writeLock().unlock(); }
						if(child1!=p){ child1.RWLock_Node.writeLock().unlock(); }
						continue; 
					}
				if(child1.left!=null){
					Node child1_Left=child1.left;
					Node L_parent=child1_Left.parent;
					if(child1_Left!=p&&!child1_Left.RWLock_Node.writeLock().tryLock()){ 
						if(nParent!=p){ nParent.RWLock_Node.writeLock().unlock();}
						 if(child1!=p){ child1.RWLock_Node.writeLock().unlock(); }
						continue;
					}
					if((child1_Left!=child1.left)||(child1_Left.mark)||(L_parent!=child1_Left.parent)){
						 if(nParent!=p){ nParent.RWLock_Node.writeLock().unlock(); }
						 if(child1_Left!=p){ child1_Left.RWLock_Node.writeLock().unlock();}
						  if(child1!=p){ child1.RWLock_Node.writeLock().unlock(); }
						 continue; 
					}
				}
			}
			
			return true;
		}
	}
	//System.out.println("Value of i:"+i);
	return false;
}
void updateChild(Node parent, Node oldCh, Node newCh)
{
	if(parent.left==oldCh)
	{
		parent.left=newCh;
	}
	else
	{
		parent.right=newCh;
	}
	if(newCh!=null){ newCh.parent=parent; }
}
}






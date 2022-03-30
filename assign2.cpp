//***********************************
// Marlee Bryant
// Assignment 2 - Concurrency Control
// CS 609 Database Systems
//***********************************

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
using namespace std;

class VariableLock;

// variable/data item for MVCC
class Variable {
  public:
    string name;
    int verNum;
    int rTS = -1;
    int wTS = -1;
};

// transaction
class TransVar {
  public:
    int num;
    int timestamp;
    int abort = 0;
    int block = 0;
    vector<string> opsAfterBlock;
    vector<string> ops;
    vector<VariableLock> varsLocked;
};

//variable/data item for 2PL
class VariableLock {
  public:
    string name;
    int wLock = 0;
    int rLock = 0;
    TransVar * holdTran;
    vector<TransVar> requestTrans;
};


//************************************************
// Strong Strict Two Phase Locking with Wound Wait
//************************************************

int twoPL(string sched, vector<TransVar> bTrans = vector<TransVar>(), vector<VariableLock> bVars = vector<VariableLock>()) {
  vector<TransVar> trans = bTrans;
  vector<VariableLock> vars = bVars;
  string operation;
  int tsInc = 0;
  string opVar;
  int opTran;
  int vFound = 0;
  int restarts = 0;
  stringstream ss;
  int tLoc;
  int vLoc;
  ss.str(sched);

  while(getline(ss, operation, ';')) {
    vFound = 0;
    if(operation.size() < 2)
      break;
    // Case for start of transaction
    if(operation.at(0) == 'S'){
      // Create new transaction object
      TransVar newTran;
      newTran.num = operation.at(1) - '0';
      newTran.timestamp = tsInc;
      newTran.ops.push_back(operation);
      tsInc++;
      trans.push_back(newTran);
      cout << operation << " ";
    }

    // Case for read
    else if(operation.at(0) == 'R'){
      opVar = operation.at(3);
      opTran = operation.at(1) - '0';

      // find the appropriate transaction object
      for(int i = 0; i < trans.size(); i++) {
        if( opTran == trans.at(i).num)
          tLoc = i;
      }

      trans.at(tLoc).ops.push_back(operation);
      if(trans.at(tLoc).abort == 0 && trans.at(tLoc).block == 0) {

        // Check if there is an object for the variable
        // find the appropriate transaction object
        for(int i = 0; i < vars.size(); i++) {
          if( opVar == vars.at(i).name) {
            vLoc = i;
            vFound = 1;
          }
        }

        // If no object exists for this variable create one
        if(vFound == 0) {
          VariableLock newVar;
          newVar.name = opVar;
          vars.push_back(newVar);
          vLoc = vars.size() - 1;
        }

        // Variable has write lock
        if(vars.at(vLoc).wLock == 1) {
          // If the requesting transaction holds the lock
          if(vars.at(vLoc).holdTran->num == trans.at(tLoc).num)
            cout << operation << " ";

          // If a different transaction holds the lock
          else {
            // If the requesting transaction is older than the transaction
            // holding the lock
            if(trans.at(tLoc).timestamp < vars.at(vLoc).holdTran->timestamp) {
              cout << "A" << vars.at(vLoc).holdTran->num << " ";
              vars.at(vLoc).holdTran->abort = 1;
              for(int i = 0; i < vars.at(vLoc).holdTran->varsLocked.size(); i++) {
                if( vars.at(vLoc).holdTran->varsLocked.at(i).name == opVar) {
                  vars.at(vLoc).holdTran->varsLocked.erase(vars.at(vLoc).holdTran->varsLocked.begin() + i);
                }
              }
              vars.at(vLoc).holdTran = &trans.at(tLoc);
              trans.at(tLoc).varsLocked.push_back(vars.at(vLoc));
              cout << operation << " ";
              restarts++;
            }

            // If the transaction holding the lock is older than the
            // requesting transaction
            else {
              cout << "B" << trans.at(tLoc).num << " ";
              trans.at(tLoc).block = 1;
              trans.at(tLoc).opsAfterBlock.push_back(operation);
              vars.at(vLoc).requestTrans.insert(vars.at(vLoc).requestTrans.begin(), trans.at(tLoc));
            }
          }
        }

        // Variable has read lock
        else if(vars.at(vLoc).rLock == 1) {
          cout << operation << " ";
        }

        // Variable is unlocked
        else {
          vars.at(vLoc).rLock = 1;
          vars.at(vLoc).holdTran = &trans.at(tLoc);
          trans.at(tLoc).varsLocked.push_back(vars.at(vLoc));
          cout << operation << " ";
        }
      }

      // If transaction is blocked, keep track of operations after block
      else if(trans.at(tLoc).abort == 0 && trans.at(tLoc).block == 1) {
        trans.at(tLoc).opsAfterBlock.push_back(operation);
      }
    }

    // Case for Write
    else if(operation.at(0) == 'W'){
      opVar = operation.at(3);
      opTran = operation.at(1) - '0';

      // find the appropriate transaction object
      for(int i = 0; i < trans.size(); i++) {
        if( opTran == trans.at(i).num)
          tLoc = i;
      }

      trans.at(tLoc).ops.push_back(operation);
      if(trans.at(tLoc).abort == 0) {

        // Check if there is an object for the variable
        // find the appropriate transaction object
        for(int i = 0; i < vars.size(); i++) {
          if( opVar == vars.at(i).name) {
            vLoc = i;
            vFound = 1;
          }
        }

        // If no object exists for this variable create one
        if(vFound == 0) {
          VariableLock newVar;
          newVar.name = opVar;
          vars.push_back(newVar);
          vLoc = vars.size() - 1;
        }

        // Variable has lock
        if(vars.at(vLoc).wLock == 1 || vars.at(vLoc).rLock == 1) {
          // If the requesting transaction holds the lock
          if(vars.at(vLoc).holdTran->num == trans.at(tLoc).num) {
            cout << operation << " ";
            if(vars.at(vLoc).wLock == 0) {
              vars.at(vLoc).wLock = 1;
              vars.at(vLoc).rLock = 0;
            }
          }

          // If a different transaction holds the lock
          else {
            // If the requesting transaction is older than the transaction
            // holding the lock
            if(trans.at(tLoc).timestamp < vars.at(vLoc).holdTran->timestamp) {
              cout << "A" << vars.at(vLoc).holdTran->num << " ";
              vars.at(vLoc).holdTran->abort = 1;
              for(int i = 0; i<vars.at(vLoc).holdTran->varsLocked.size(); i++) {
                if( vars.at(vLoc).holdTran->varsLocked.at(i).name == opVar) {
                  vars.at(vLoc).holdTran->varsLocked.erase(vars.at(vLoc).holdTran->varsLocked.begin() + i);
                }
              }
              vars.at(vLoc).holdTran = &trans.at(tLoc);
              trans.at(tLoc).varsLocked.push_back(vars.at(vLoc));

              cout << operation << " ";
              restarts++;
            }

            // If the transaction holding the lock is older than the
            // requesting transaction
            else {
              cout << "B" << trans.at(tLoc).num << " ";
              trans.at(tLoc).block = 1;
              trans.at(tLoc).opsAfterBlock.push_back(operation);
              vars.at(vLoc).requestTrans.insert(vars.at(vLoc).requestTrans.begin(), trans.at(tLoc));
            }
          }
        }

        // Variable is unlocked
        else {
          vars.at(vLoc).wLock = 1;
          vars.at(vLoc).holdTran = &trans.at(tLoc);
          trans.at(tLoc).varsLocked.push_back(vars.at(vLoc));
          cout << operation << " ";
        }
      }
    }
  }

  // commit unaborted transactions and release locks, allowing blocked
  // transactions to complete
  for(TransVar t : trans) {
    if(t.abort == 0 && t.block == 0) {
      cout << "C" << t.num << " ";

      for(VariableLock v : t.varsLocked) {
        for( VariableLock v2 : vars) {
          if( v2.name == v.name) {
            v = v2;
          }
        }
        v.rLock = 0;
        v.wLock = 0;
        if(v.requestTrans.size() > 0) {
          TransVar newHolder = v.requestTrans.back();
          v.requestTrans.pop_back();
          newHolder.block = 0;
          for(string op : newHolder.opsAfterBlock) {
            cout << op << " ";
          }
          cout << "C" << newHolder.num << " ";
        }
      }
    }
  }


  // serially complete aborted transactions
  for(TransVar t : trans) {
    if( t.abort == 1 ) {
      for(string op : t.ops)
        cout << op << " ";
      cout << "C" << t.num << " ";
    }
  }

  cout << endl;
  return restarts;
}


//*********************************
// Multiversion Concurrency Control
//*********************************

int mvcc(string sched, int &versions) {
  vector<TransVar> trans;
  vector<Variable> vars;
  string operation;
  int tsInc = 0;
  string opVar;
  int opTran;
  int tLoc;
  Variable varInfo;
  int vFound = 0;
  int maxVerNum = 0;
  int restarts = 0;
  stringstream ss;
  ss.str(sched);

  while(getline(ss, operation, ';')) {
    vFound = 0;
    maxVerNum = 0;
    if(operation.size() < 2)
      break;

    // Case for start of transaction
    if(operation.at(0) == 'S'){
      // Create new transaction object
      TransVar newTran;
      newTran.num = operation.at(1) - '0';
      newTran.timestamp = tsInc;
      newTran.ops.push_back(operation);
      tsInc++;
      trans.push_back(newTran);
      cout << operation << " ";
    }

    // Case for Read
    else if(operation.at(0) == 'R'){
      opVar = operation.at(3);
      opTran = operation.at(1) - '0';

      // find the appropriate transaction object
      for(int i = 0; i < trans.size(); i++) {
        if( opTran == trans.at(i).num)
          tLoc = i;
      }

      trans.at(tLoc).ops.push_back(operation);
      if(trans.at(tLoc).abort == 0) {

        // Check if there are any versions of the variable
        for(Variable v : vars) {
          if( opVar == v.name)
            vFound = 1;
        }

        // If no versions of this variable exist, create the first one
        // and read from it
        if(vFound == 0) {
          Variable newVar;
          newVar.name = opVar;
          newVar.rTS = trans.at(tLoc).timestamp;
          newVar.verNum = 0;

          vars.push_back(newVar);
          varInfo = newVar;
          cout << operation.substr(0,4) << ", V0) ";
          versions++;
        }

        // If versions do exist, find the one with the largest last written
        // timestamp less than or equal to the transaction's timestamp and
        // read from it
        else {
          varInfo.wTS = -1;
          varInfo.rTS = -1;
          varInfo.verNum = -1;
          for(Variable v : vars) {
            if(v.wTS <= trans.at(tLoc).timestamp && v.name == opVar && v.wTS >= varInfo.wTS) {
              varInfo = v;
            }
          }

          if(varInfo.rTS <= trans.at(tLoc).timestamp)
            varInfo.rTS = trans.at(tLoc).timestamp;

          cout << operation.substr(0,4) << ", V" << varInfo.verNum << ") ";
        }
      }
      for(Variable v: vars) {
        if(v.name == varInfo.name) {
          v = varInfo;
        }
      }
    }

    // Case for Write
    else if(operation.at(0) == 'W'){
      opVar = operation.at(3);
      opTran = operation.at(1) - '0';

      // find the appropriate transaction object
      for(int i = 0; i < trans.size(); i++) {
        if( opTran == trans.at(i).num)
          tLoc = i;
      }

      trans.at(tLoc).ops.push_back(operation);
      if(trans.at(tLoc).abort == 0) {

        // Check if there are any versions of the variable
        for(Variable v : vars) {
          if( opVar == v.name) {
            vFound = 1;
          }
        }

        // If no versions of this variable exist, create the first one
        // and write to it
        if(vFound == 0) {
          Variable newVar;
          newVar.name = opVar;
          newVar.wTS = trans.at(tLoc).timestamp;
          newVar.verNum = 0;

          vars.push_back(newVar);
          varInfo = newVar;
          cout << operation.substr(0,4) << ", V0 from initial) ";
          versions++;
        }

        // If versions do exist, find the one with the largest last written
        // timestamp less than or equal to the transaction's timestamp and
        // write to it, creating a new version of the variable, or abort
        else {
          varInfo.wTS = -1;
          varInfo.rTS = -1;
          varInfo.verNum = -1;
          for(Variable v : vars) {
            if(v.wTS <= trans.at(tLoc).timestamp && v.name == opVar && v.wTS >= varInfo.wTS)
              varInfo = v;
          }

          if(trans.at(tLoc).timestamp >= varInfo.rTS) {
            Variable newVar;
            newVar.name = varInfo.name;
            newVar.wTS = trans.at(tLoc).timestamp;

            for(Variable v : vars) {
              if(v.name == varInfo.name){
                maxVerNum++;
              }
            }
            newVar.verNum = maxVerNum;
            vars.push_back(newVar);
            cout << operation.substr(0,4) << ", V" << newVar.verNum << " from V" << varInfo.verNum << ") ";
            versions++;
          }

          else {
            cout << "A" << trans.at(tLoc).num << " ";
            trans.at(tLoc).abort = 1;
            restarts++;
          }
        }
      }
      for(Variable v: vars) {
        if(v.name == varInfo.name) {
          v = varInfo;
        }
      }
    }
  }

  // Commit unaborted transactions
  for(TransVar t : trans) {
    if(t.abort == 0)
      cout << "C" << t.num << " ";
  }

  // serially complete aborted transactions
  for(TransVar t : trans) {
    if(t.abort == 1) {
      for(string op : t.ops)
        cout << op << " ";
      cout << "C" << t.num << " ";
    }
  }

  cout << endl;
  return restarts;
}



int main() {

  vector<string> scheds;
  vector<int> numVer;
  vector<int> restartMVCC;
  vector<int> restart2PL;

  // file opening and reading by line, each line is a schedule
  ifstream schedfile("schedule.txt");
  string sched;
  while(getline( schedfile, sched)) {
    scheds.insert(scheds.begin(), sched);
  }

  // simulating each schedule using MVCC and Strong Strict 2PL with Wound Wait
  // storing versions created and number of restarts for summary
  int versions;
  int reNumMVCC;
  int reNum2PL;
  string currSched;
  for(int i = 0; i < scheds.size(); i++) {
    versions = 0;
    currSched = scheds.back();
    scheds.pop_back();
    reNumMVCC = mvcc(currSched, versions);
    reNum2PL = twoPL(currSched);
    restartMVCC.insert(restartMVCC.begin(), reNumMVCC);
    restart2PL.insert(restart2PL.begin(), reNum2PL);
    numVer.insert(numVer.begin(), versions);
  }

  // display summary for each schedule including restarts for both methods
  // and versions created for MVCC only
  for(int i = 0; i < scheds.size(); i++) {
    cout << "Schedule " << i+1 << ":"<< endl;
    cout << "  MVCC restarts: " << restartMVCC.back();
    cout << "  MVCC versions: " << numVer.back();
    cout << "  2PL restarts: " << restart2PL.back() << endl;
    restartMVCC.pop_back();
    restart2PL.pop_back();
    numVer.pop_back();
  }

}

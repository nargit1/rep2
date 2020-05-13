import requests
import json
import pandas as pd
import numpy as np
import copy
import time
import datetime
import pandas.core.strings as ps
from pandas.core.accessor import CachedAccessor
#import pandas.core.groupby.generic as pg 


_iexpr = ""

#class to provide functions with IICS equivalents
class cexp:
    def __init__(self,col):
        self.col = col 
        global _iexpr
        _iexpr = col.name
        self.thisexpr = col.name
        self.colname = col.name
        
    def __add__(self, o): 
        print (type(o))
        if (type(o) == cexp):
            self.col =  self.col + o.col 
            self.thisexpr = self.thisexpr +  "+" + o.thisexpr
        else:
            self.col =  self.col + o 
            self.thisexpr = self.thisexpr +  "+'" + o + "'"          
        return self

        
    def value(self):
        global _iexpr
        _iexpr = self.thisexpr
        return self.col   
    
    
    
    def upper(self):
        self.thisexpr = "upper("+self.thisexpr+")"
        self.col = self.col.str.upper()
        return self
    
    def lower(self):
        _iexpr = "lower("+self.thisexpr+")"
        self.thisexpr = _iexpr
        self.col = self.col.str.lower()
        return self
    
    def replace(self,sold,snew):
        self.thisexpr = "replacestr("+self.thisexpr+",1,'"+sold+"','"+snew+"')"
        self.col = self.col.str.replace(sold,snew)
        return self
    
    def substring(self,i,j):
        self.thisexpr = "substring("+self.thisexpr+","+str(i)+","+str(j)+")" 
        self.col =  self.col.str[i:j]
        return self
    
    def to_date(self,p='mm/dd/yyyy'):
        self.thisexpr = "To_Date("+self.thisexpr+",'"+p+"')"
        self.col =  pd.to_datetime(self.col)
        return self


class iexp:
    
    def __init__(self):
        self.val = ""
    def __str__(self):
        return "{} ".format(self.__class__.__name__)
        
    def upper(self,s):
        return s.upper()
    
    def lower(self,s):
        return s.lower()    
    
    def substring(self,s,i,j):
        return s[i:j]
    
    def concat(self,s1,s2):
        return s1+s2
    
    def replacestr(self,flag,s,sold,snew):
        return s.replace(sold,snew)
    
    def to_integer(self,s):
        return s   #int(s)
    
    def to_date(self,d1,p='mm/dd/yyyy'):
        return pd.to_datetime(d1)
    
    
    def date_diff(self,d1,d2,p='mm/dd/yyyy'):
        return pd.to_datetime(d1) - pd.to_datetime(d2)
    
    def iif(self,cond,t,f):
        return np.where(cond,t,f)
    
   
    def iics(self,ex):
        return str(ex)

            
    def cnvexpr_toiics(self,ex):
        ex= ex.replace ("ix.iics","")
        ex= ex.replace ("ix.","")
        ex= ex.replace ("x.","")
        ex= ex.replace ("x['","")
        ex= ex.replace ("'].str","")
        ex= ex.replace ("']","")
        ex= ex.replace ("==","=")
        ex= ex.replace ("df1['","")
        ex= ex.replace ("df['","")
        ex= ex.replace ("self['","")
        ex= ex.replace("lambda x:","") 
        ex= ex.replace("np.where","IIF") 
        return ex    
    
    def cnvfilter_toiics(self,fl):
        fl = fl.replace("==","=")
        fl = fl.replace("&"," AND ")
        fl = fl.replace("|"," OR ")
        #fl = fl.replace("To_Integer","")   #???
        return fl   
    
    def cnvfilter_topandas(self,exp):
        exp = exp.replace('to_integer','')
        exp = exp.replace('to_date','')
        exp = exp.replace('To_Integer','')
        exp = exp.replace('To_Date','')        
        exp = exp.replace(' AND ',' & ')
        exp = exp.replace(' OR ',' | ')
        return exp    
    
    def cnvgpb_toiics(self,gpb):
        gpb = gpb.replace("[","")
        gpb = gpb.replace("]","")
        gpb = gpb.replace("'","")
        gpb = gpb.replace(" ","")
        gpb = gpb.replace(",",";")
        return gpb
    
    def cnvagg_toiics(self,agg,thisdt):
        #observe_max=('observed','max')
        c = str(agg[0])
        f = str(agg[1])
        estack.print(str(agg))
        if (thisdt.lower().find("integer")>=0):
            c = "To_Integer("+c+")"
        if (thisdt.lower().find("double")>=0):
            c = "To_Double("+c+")"            
        exp = f+"("+c+")"
        return exp       

    def cnvsort_toiics(self,srt,ascending):
        srt = srt.replace("[","")
        srt = srt.replace("]","")
        srt = srt.replace("'","")
        if (ascending):
            srt = srt.replace(";","=ASC;")+"=ASC"
        else:
            srt = srt.replace(";","=DESC;")+"=DESC"    
        return srt  
    
    def fillna_toiics(self,s):
        exp = "";
        if  (type(s) != CDISeries):
            exp = 'string (10,0) Declare_%enums%={\\"port\\":\\"All Ports\\"};' 
            exp = exp + "string(0,0) %port%_EX = iif(ISNULL(%port%),'" + s + "',%port%)"
        else:
            #call dataobj setenum_fromseries to setup the enums before calling this
            exp = "string(0,0) %port%_EX = iif(ISNULL(%port%),'%portval%',%port%)"
        return exp
          
 
        
ix = iexp()

_lake_connection = "000HI50B00000000000B"


class expStack:
    def __init__(self):
        self.items = []   
        self.debug = False
        self.info = False
        
    def dubug(self,b):
        self.debug = b
        
    def info(self,b):
        self.info = b     
        
    def skip(self,b):
        global _iskip
        _iskip = b
        
    def apply(self,b):
        global _iskip
        _iskip = not b        
        
    def print(self,str):
        if (not _iskip) and (self.debug):
            print(str)
        
    def printi(self,str):
        if (self.info or self.debug):
            print(str)            

    def pushexp (self):
        global _iexpr
        if (_iskip):
            _iexpr=""
            return 0
        if (_iexpr != ""):
            if  self.debug:
                print ('push '+str(_iexpr))
            self.items.append(str(_iexpr))
            _iexpr = ""
        return 0
   
    def isempty(self):
        if (len(self.items) > 0):
            return False
        else:
            return True
        
    def popexp (self):
        if (_iskip):
            return ""       
        if (len(self.items) == 0):
            print ("expr stack empty!")
            return "<VAL>"
        ex = self.items.pop()
        if  self.debug:
            print ('pop '+ex)                
        return ex
    
    #pop the final expr. if more items exist then get them
    def popfinalexp (self):
        #if (_iskip):
        #    return ""
        if (len(self.items) == 0):
            return ""
        ex = self.items.pop()  # self.popexp()
        self.print('pop final '+ex)
        if (len(self.items) > 0):
            #ex = "FUNC("+ex + ","+ self.popexp() + ")" 
            self.print('Extra items in stack '+  self.popexp())
        self.clear()
        return ex    
    
    def peek(self):
         return self.items[len(self.items)-1]
    
    def clear(self):
        global _iskip
        global _iexpr
        self.print("Clear stack called")
        _iexpr = ""
        self.items = []
        #iskip= False
            
estack = expStack()  

_iskip= False
_iskiprhs = False

class CDIStringMethods(ps.StringMethods):
    def __init__(self, *args, **kwargs):
        super(CDIStringMethods, self).__init__(*args, **kwargs)
        
    def _wrapped_pandas_method(self, mtd, *args, **kwargs):
        val = getattr(super(CDIStringMethods, self), mtd)(*args, **kwargs)
        if type(val) == pd.Series:
            val.__class__ = CDISeries
        return val     
    
    def process_method(self,md):
        global _iexpr
        ex = estack.popexp()
        _iexpr = md+"("+ex+")"
        estack.pushexp()        
    
    def upper(self):
        self.process_method("upper")
        return self._wrapped_pandas_method("upper")
        
    def lower(self):
        self.process_method("lower")
        return self._wrapped_pandas_method("lower")       
    
    def title(self):
        self.process_method("initcap")
        return self._wrapped_pandas_method("title") 
    
    def replace(self, pat, repl, n=-1, case=None, flags=0, regex=True):
        global _iexpr
        estack.pushexp()
        ex = estack.popexp()
        _iexpr = "replacestr("+ex+",'"+pat+"','"+repl+"')"
        estack.pushexp()
        return self._wrapped_pandas_method ("replace", pat=pat, repl=repl, n=n, case=case, flags=flags, regex=regex)
    
    def contains(self, pat, case=True, flags=0,  regex=True):     #leave out na=nan
        global _iexpr
        estack.pushexp()
        ex = estack.popexp()
        _iexpr = "instr("+ex+",'"+pat+"')"
        estack.pushexp()        
        return self._wrapped_pandas_method ("contains", pat=pat, case=case, flags=flags, regex=regex)
    
    #use this instead of [i:j] for now. 
    #call iics substr()
    def substring(self,i,j):
        global _iexpr
        ex = estack.popexp()
        _iexpr =  "substr("+ex+","+str(i+1)+","+str(j-i)+")" 
        estack.pushexp()  
        return CDISeries(self[i:j])
    
    def len(self):
        self.process_method("length")
        return self._wrapped_pandas_method("len")
        
    
    #regex extract
    def extract(self, pat, flags=0, expand=True):
        global _iexpr
        col = estack.popexp()
        pat1 = str(pat)
        pat1 = pat1.replace('\\','\\\\')  
        _iexpr =  "reg_extract("+col+",'"+pat1+"')"
        estack.pushexp()          
        return self._wrapped_pandas_method ("extract", pat=pat, flags=flags, expand=expand)

'''   
class CDIGroupBy(pg.DataFrameGroupBy):
    def __init__(self, obj, *args, **kwargs):
        super(CDIGroupBy, self).__init__(obj,*args, **kwargs)
        self.pnt = obj
    
    def agg(self, *args, **kwargs):
        estack.print("in agg")
        gbcols = estack.popexp()
        estack.print ("group by-"+gbcols)
        self.pnt.dataobj.groupby(gbcols)
        for col, exp in kwargs.items():
            curcols = str(self.pnt.dataobj.dfc.index.values)
            thisdt=""
            if (curcols.find("'"+col+"'") >0): 
                thisdt = str(self.pnt.dataobj.dfc.at[col,'dt'])
            aggexp = ix.cnvagg_toiics(exp,thisdt)
            #print (aggexp)
            self.pnt.dataobj.dfc.at[col,'ex'] = aggexp
            #self.dataobj.dfc.at[col,'dt'] = "" 
        global _iskip
        _iskip == True
        df= CDIDataFrame (super(CDIGroupBy, self).agg(*args, **kwargs))
        df.dataobj = self.pnt.dataobj
        _iskip == False
        estack.clear()
        return df
'''    
    

class CDISeries(pd.Series):
    
    #got this from pandas example
    str = CachedAccessor("str", CDIStringMethods)
    
    def __init__(self, *args, **kwargs):
        super(CDISeries, self).__init__(*args, **kwargs) 
        
    def __getitem__(self, key):
        estack.print ('_get series item '+str(key))
        return super(CDISeries, self).__getitem__(key)    
   
    def __setitem__(self, key, value):
        estack.print ("_set series called "+str(key))
        super(CDISeries, self).__setitem__(key, value)    

    def __finalize__(self, other, method=None, **kwargs):
        estack.print ("_finalize series called "+str(method))  
        #estack.print ("other class: "+str(other.__class__))  

        return self          
    
        
    @property
    def _constructor(self):      
        return CDISeries
    
       
    def __add__(self, other): 
        rslt = CDISeries(pd.Series(self).add(other))
        self.process_operator(other,'+')
        return rslt
    
    def __radd__(self, other): 
        rslt = CDISeries(pd.Series(self).radd(other))
        self.process_operator(other,'+',reverse=True)
        return rslt    
    
    def __sub__(self, other): 
        rslt = CDISeries(pd.Series(self).sub(other))
        self.process_operator(other,'-')
        return rslt    
    
    def __rsub__(self, other): 
        rslt = CDISeries(pd.Series(self).rsub(other))
        self.process_operator(other,'-',reverse=True)
        return rslt        
    
    def __truediv__(self, other): 
        rslt = CDISeries(pd.Series(self).div(other))
        self.process_operator(other,'\\')
        return rslt  
    
    def __div__(self, other): 
        rslt = CDISeries(pd.Series(self).div(other))
        self.process_operator(other,'\\')
        return rslt   
    
    def __rdiv__(self, other): 
        rslt = CDISeries(pd.Series(self).div(other))
        self.process_operator(other,'\\',reverse=True)
        return rslt                              
   
    def __mul__(self, other): 
        rslt = CDISeries(pd.Series(self).mul(other))
        self.process_operator(other,'*')
        return rslt     
    
    def __rmul__(self, other): 
        rslt = CDISeries(pd.Series(self).mul(other))
        self.process_operator(other,'*',reverse=True)
        return rslt          
                              
    def __lt__(self, other, level=None, fill_value=None, axis=0):
        rslt = CDISeries(pd.Series(self).lt(other,level=level, fill_value=fill_value, axis=axis))
        self.process_operator(other,'<')
        return rslt    
    
    def __gt__(self, other, level=None, fill_value=None, axis=0):
        rslt = CDISeries(pd.Series(self).gt(other,level=level, fill_value=fill_value, axis=axis))
        self.process_operator(other,'>')
        return rslt    
    
    def __eq__(self, other, level=None, fill_value=None, axis=0):
        rslt = CDISeries(pd.Series(self).eq(other,level=level, fill_value=fill_value, axis=axis))
        self.process_operator(other,'=')
        return rslt     
    def __ne__(self, other, level=None, fill_value=None, axis=0):
        rslt = CDISeries(pd.Series(self).ne(other,level=level, fill_value=fill_value, axis=axis))
        self.process_operator(other,'!=')
        return rslt      
    
    def __and__(self, other):
        rslt = CDISeries(pd.Series(self) & pd.Series(other))
        estack.print ('process AND')
        self.process_operator(other,' AND ')
        return rslt   
    def __or__(self, other):
        rslt = CDISeries(pd.Series(self) or pd.Series(other))
        self.process_operator(other,' OR ')
        return rslt      
                              
    def _wrapped_pandas_method(self, mtd, *args, **kwargs):
        val = getattr(super(CDISeries, self), mtd)(*args, **kwargs)
        if type(val) == pd.Series:
            val.__class__ = CDISeries
        return val    
    
    def process_operator(self,other,opt,reverse=False):
        global _iexpr
        estack.pushexp()
        if (type(other) != CDISeries):
            #handle constants like 'abc' or numbers like 125
            if (type(other) == str):
                _iexpr = "'"+str(other)+"'"    
            else:
                _iexpr = str(other)
            estack.pushexp()
        ex = estack.popexp()
        ex2 = estack.popexp()
        if (not reverse):
            _iexpr = "(" + ex2  +  opt + " "+  ex + ")"
        else:
            _iexpr = "(" + ex +  opt + " " +  ex2 + ")"
        estack.pushexp()       

    
    def add(self, other, level=None, fill_value=None, axis=0):
        self.process_operator(other,'+')
        return self._wrapped_pandas_method("add", other,fill_value=fill_value,axis=axis)    
    
    def sub(self, other, level=None, fill_value=None, axis=0):
        self.process_operator(other,'-')
        return self._wrapped_pandas_method("sub", other,level=level,fill_value=fill_value,axis=axis) 
    
    def div(self, other, level=None, fill_value=None, axis=0):
        self.process_operator(other,'\\')
        return self._wrapped_pandas_method("div",other,level=level,fill_value=fill_value,axis=axis) 
    
    def mul(self, other, level=None, fill_value=None, axis=0):
        self.process_operator(other,'*')
        return self._wrapped_pandas_method("mul",other,level=level,fill_value=fill_value,axis=axis)  
    
    def gt(self, other, level=None, fill_value=None, axis=0):
        self.process_operator(other,'>')
        return self._wrapped_pandas_method("gt", other, level=None, fill_value=None, axis=0)
    
    def lt(self, other, level=None, fill_value=None, axis=0):
        self.process_operator(other,'<')
        return self._wrapped_pandas_method("lt", other, level=None, fill_value=None, axis=0)
    
    def isin(self, values):
        col = estack.popexp()
        estack.print("in series-isin "+col)
        exp1 = "in("+col
        for val in values:
            exp1 = exp1+",'"+val+"'"
        exp1 = exp1 + ")"
        global _iexpr
        _iexpr = exp1
        estack.pushexp() 
        return self._wrapped_pandas_method("isin",values=values)
        
    def isna(self):
        col = estack.popexp()
        estack.print("in series-isna "+col)
        exp1 = "isnull("+col+")"
        global _iexpr
        _iexpr = exp1
        estack.pushexp() 
        return self._wrapped_pandas_method("isna")
    
    def fillna(self, value=None, method=None, axis=None, inplace=False, limit=None, downcast=None):
        col = estack.popexp()
        col = col.replace("To_Integer","")  # need to remove this cast for null checks.
        estack.print("series-in fillna-"+str(method))
        #exp1 = "iif(ISNULL("+col+"),'"+str(value)+"',"+col+")"       
        exp1=""
        if (type(value) == dict):   # value list. from groupby operation fillna(groupby.mean())
            gbc =  estack.popexp()
            expf = '%col%'
            for grp in value:
                valt = value[grp]
                expcurr = "iif (ISNULL("+col+") and "+gbc+"='"+str(grp)+"','"+str(valt)+"',%col%)"
                expf = expf.replace('%col%',expcurr)
            exp1 = expf       
        else:  #single value
            exp1 = "iif(ISNULL("+col+"),'"+str(value)+"',"+col+")"
        
        global _iexpr
        _iexpr = exp1
        estack.pushexp() 
        return self._wrapped_pandas_method("fillna", value=value, method=method, axis=axis, inplace=inplace, limit=limit, downcast=downcast)
    
    #conditional assignments  df['S'] = df['S'].str.lower().where(df['S'] != 'male',35)
    #  df[col]  = df[col].where(cond,value)
    def where(self, cond, other, inplace=False, axis=None, level=None, errors='raise', try_cast=False):
        estack.print ("series-in where")
        #pop the arguments
        cond1 = estack.popexp()
        expt = estack.popexp()
        expf=""
        if (other.__class__ == CDISeries):
            expf =  estack.popexp()
        else:
            expf = "'"+str(other)+"'"   #constants are strings
        estack.clear() 
        global _iexpr
        _iexpr = "iif("+cond1+","+expt+","+expf+")"
        estack.pushexp()
        global _iskip
        _iskip= True  
        s1 =  self._wrapped_pandas_method("where", cond, other=other, inplace=inplace, axis=axis, level=level, errors=errors, try_cast=try_cast)
        _iskip= False
        return s1
    
    #df[col] = df[col].map(dict)
    # {'A':'1','B':'2'}
    #use IICS decode statement
    def map(self, arg, na_action=None):
        col = estack.popexp()
        if (type(arg) == dict):  #  {'A':'1','B':'2'}
            exp = "decode("+col+","
            for fval in arg:
                tval = arg[fval]
                exp = exp + "'"+str(fval)+"','"+str(tval)+"',"
            exp = exp[0:len(exp)-1]  #remove extra comma
            exp = exp + ")" 
            estack.clear()
            global _iexpr
            _iexpr = exp
            estack.pushexp
        return self._wrapped_pandas_method("map",arg=arg,na_action=na_action)
    
    #df[] = df[].replace(list,repl)  e.g df.replace([0, 1, 2, 3], 4)
    def replace(self, to_replace=None, value=None, inplace=False, limit=None, regex=False, method='pad'):
        col = estack.popexp() 
        torep = ""
        exp="replacestr()"
        if (type(to_replace) == list):
            for item in to_replace:
                torep = torep+"'"+str(item)+"',"
            exp = "replacestr(0,"+col+","+torep+"'"+str(value)+"')"
        elif (type(to_replace) == str):
            torep = "'"+to_replace+"'"
            exp = "replacestr(0,"+col+","+torep+"'"+str(value)+"')"
        estack.clear()
        global _iexpr
        _iexpr = exp
        estack.pushexp            

        return self._wrapped_pandas_method("replace",to_replace=to_replace, value=value, inplace=inplace, limit=limit, regex=regex, method=method)
      

#Subclass from Pandas DataFrame.  Calls IICS dataobject methods and the base dataframe methods
class CDIDataFrame(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super(CDIDataFrame, self).__init__(*args, **kwargs) 
        #reference to IICS dataobject 
        self.dataobj = None

    #
    # Implement pandas methods
    #
    @property
    def _constructor(self):
        return CDIDataFrame

    def __getitem__(self, key):
        if (_iskip == False):
            estack.print ('get item '+str(key))
            global _iexpr
            if (type(key) == str):
                col = str(key)
                dt= str(self.dataobj.dfc.at[col,'dt'])
                if (dt.lower().find('integer')>=0):  #set datatype if needed
                    col = "To_Integer("+col+")"
                _iexpr = col
                estack.pushexp()        
        result = super(CDIDataFrame, self).__getitem__(key)    
        return result
    
    def __setitem__(self, key, value):
        estack.print ("_set called "+str(key))
        if (_iskip == False):
            #handle cases such as df[a] = 'x' 
            if (type(value) != CDISeries):
                if (estack.isempty()):
                    global _iexpr
                    _iexpr = "'"+str(value)+"'"
            self.getassign(key)
        else:
            if (_iskiprhs == True):   #make exception for skipping RHS only
                self.getassign(key)
        super(CDIDataFrame, self).__setitem__(key, value)    

    def __finalize__(self, other, method=None, **kwargs):
        global _iskip
        estack.print ("_finalize called "+str(method))  
        if (method == None):
            self.dataobj = other.dataobj
            if (_iskip == False):
                self.getfilter()
            else:
                if (_iskiprhs == True):   #make exception for skipping RHS only    
                    self.getfilter()
        estack.clear()
        return self  
   
   # this is needed to use the custom CDISeries derived from Panda Series             
    _constructor_sliced = CDISeries
    
                
    def copy(self, deep=True):
        print("copy called")
        data = self._data
        dataobj = self.dataobj
        if deep:
            data = data.copy()
        return CDIDataFrame(data).__finalize__(self) 
    
    #capture a column assignment df['abc'] = 'xyz'
    def getassign(self,col):
        estack.pushexp()  #push any expression
        #turn off skip rhs
        global _iskiprhs
        _iskiprhs = False        
        expr = estack.popfinalexp()
        if (expr == "_skipit_"):
            estack.print("Skip assignement for "+col)
            return
        if (expr == ""):
            estack.printi("No RHS for "+col)
            return        
        #handle case where this is called from a IIF statement
        #expr = expr.replace('&col&',col)
        curexpr=""
        curcols = str(self.dataobj.dfc.index.values)
        if (curcols.find("'"+col+"'") >0): 
            curexpr = str(self.dataobj.dfc.at[col,'ex'])
        if (curexpr.find('%col%') <=0):
            self.dataobj.dfc.at[col,'ex'] = expr
            estack.printi("Set "+col+"= "+expr)
        else:  #handle the case for multiple iif statements against same column
            curexpr = curexpr.replace('%col%',expr)
            self.dataobj.dfc.at[col,'ex'] = curexpr
            estack.printi("Set "+col+"= "+curexpr)
        estack.clear()
        
    #capture a column assignment df['abc'] = 'xyz'
    def getfilter(self):
        #turn off skip rhs
        global _iskiprhs
        _iskiprhs = False
        estack.pushexp()  #push any expression
        if (not estack.isempty()):
            expr = estack.popfinalexp()
            if (expr == "_skipit_"):
                estack.print("Skip assignement for "+col)
                return
            if (expr == ""):
                estack.printi("No RHS for "+col)
                return             
            #add below check to ensure valid boolean exp and 
            #to avoid adding spurious expressions- to be fixed later
            if (expr.find(' ') >=0):
                self.dataobj.filter(expr)
                estack.printi("Set Filter= "+expr)
            #hanlde reversing conditions 
            if (expr.find('_not_') ==0):
                self.dataobj.filter(expr)     
                estack.printi("Set Filter= "+expr)
        estack.clear()
        
        
    def rename(self, mapper=None, index=None, columns=None, axis=None, copy=True, inplace=False, level=None, errors='ignore'): 
        #note the swap from columns to index, since the dataobj is pivoted
        self.dataobj.setpattern('_exp_')    
        if (axis == None):
            self.dataobj.dfc.rename(mapper=mapper,index=columns,inplace=True,level=level,axis=axis,errors='ignore') 
        else:
            self.dataobj.dfc.rename(mapper=mapper,index=index,inplace=True,level=level,axis='index',errors='ignore') 
        
        cdf = CDIDataFrame(super(CDIDataFrame,self).rename(mapper=mapper, index=index, columns=columns, axis=axis, copy=copy, inplace=inplace, level=level, errors='ignore'))
        cdf.dataobj = self.dataobj      
        return cdf
    
    
    def drop_duplicates(self, subset = None, keep ='first', inplace = False, ignore_index = False):
    
        return CDIDataFrame(super(CDIDataFrame,self).drop_duplicates(subset= subset, keep=keep, inplace=inplace, ignore_index=ignore_index))
 
      
    def astype(self, dtype, copy: bool = True, errors: str = 'raise'):
        #print (dtype)
        #not calling base class method for now since pandas knows datatype 
        for col, dt in dtype.items():  
            #conver to iics datatype
            if (str(dt) == 'int64'):
                dt = 'integer(10,0)'
            self.dataobj.dfc.at[col,'dt'] = dt
            
    #df.filter(items=['cola','colb']) to keep only these columns
    def filter(self, items=None, like = None, regex = None, axis=None):
        cdf = CDIDataFrame(super(CDIDataFrame,self).filter(items=items, like = like, regex = regex, axis=axis))
        self.dataobj.dfc= self.dataobj.dfc.filter(items=items,axis=0)
        cdf.dataobj = self.dataobj
        return cdf
    
    def assign(self, **kwargs):
        i=0
        #print (kwargs)
        self.dataobj.setpattern('_exp_') 
        for col, exp in kwargs.items():
            iiexp = ix.cnvexpr_toiics(exp)
            exp = exp.replace('df[','self[')
            #check if IICS only expression.  do not process in pandas
            if (exp.find("ix.iics") >= 0):
                exp = exp.replace('ix.iics','')
                kwargs[col]= str(exp)[0:10]  
            else:
                kwargs[col]= eval(exp)
            self.dataobj.dfc.at[col,'ex'] = iiexp
            self.dataobj.dfc.at[col,'dt'] = ""
        global _iskip
        _iskip= True                
        rdf = CDIDataFrame(super(CDIDataFrame,self).assign(**kwargs))    
        rdf.dataobj = self.dataobj
        _iskip= False
        return rdf
    
    def assignto(self, **kwargs):
        i=0
        #print (kwargs)
        self.dataobj.setpattern('_exp_') 
        for col, exp in kwargs.items():
            iiexp = _iexpr  #ix.cnvexpr_toiics(exp)
            #check if IICS only expression.  do not process in pandas
            self.dataobj.dfc.at[col,'ex'] = iiexp
            self.dataobj.dfc.at[col,'dt'] = ""
        rdf = CDIDataFrame(super(CDIDataFrame,self).assign(**kwargs)) 
        rdf.dataobj = self.dataobj
        return rdf
    
    def sort_values(self, by, axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last', ignore_index=False):
        
        self.dataobj.setpattern('_sort_') 
        srt = str(by)
        srt = ix.cnvsort_toiics(srt,ascending)

        self.sortstr = srt
        self.dataobj.sortby(srt)
        global _iskip
        _iskip= True       
        df=CDIDataFrame(super(CDIDataFrame,self).sort_values(by,axis=axis,ascending=ascending,inplace=True, kind='quicksort', na_position='last',ignore_index=False))
        _iskip=False
        return df
    
 
  
    def query(self, expr, inplace=False, **kwargs):
        
        print (kwargs)
        self.dataobj.setpattern('_filter_') 
        fl = ix.cnvfilter_toiics(str(expr))
        self.dataobj.filter(fl)
        expr = ix.cnvfilter_topandas(expr)  # remove any iics sepcific tokens
        global _iskip
        _iskip= True
        df =  CDIDataFrame(super(CDIDataFrame,self).query(expr, inplace=True, **kwargs))
        _iskip= False
        return df
    
    
    
    def drop(self, labels=None, axis=0, index=None, columns=None, level=None, inplace=False, errors='raise'):
        try:
            #the following is drop columnns
            if ((axis == 1) or (columns != None)):
                self.dataobj.dfc.drop(labels,inplace=True)
            #The following is like a row filter condition.  Need to reverse the condition. 
            # train.drop(train[(train['GarageCars']>3) 
            #          & (train['SalePrice']<300000)].index).reset_index(drop=True)
            if ((columns == None) and (axis == 0)):
                estack.print ("convert drop to filter-")
                estack.clear() 
                global _iexpr
                #push _not_ indicator to reverse the filter
                _iexpr = "_not_"
                estack.pushexp()
        except:
            print('missing cols')
        cdf=  CDIDataFrame(super(CDIDataFrame,self).drop(labels=labels, axis=axis, index=index, columns=columns, level=level, inplace=inplace, errors=errors))
        cdf.dataobj = self.dataobj #copy.copy(self.dataobj)
        return cdf
        

    
    def groupby(self, by=None, axis=0, level=None, as_index: bool = True, sort: bool = True, group_keys: bool = True, squeeze: bool = False, observed: bool = False,**kwargs):
        
        self.dataobj.setpattern("_agg_",chain=False)
        
        gpb = ix.cnvgpb_toiics(str(by))
        self.groupbystr = gpb
        self.dataobj.groupby(gpb)
        #e.g observe_max=('observed','max')
        for col, exp in kwargs.items():
            thisdt = str(self.dataobj.dfc.at[col,'dt'])
            aggexp = ix.cnvagg_toiics(exp,thisdt)
            #print (aggexp)
            self.dataobj.dfc.at[col,'ex'] = aggexp
            #self.dataobj.dfc.at[col,'dt'] = ""
        global _iskip
        _iskip= True
        rdf = CDIDataFrame(super(CDIDataFrame,self).groupby(by, axis=0, level=level, sort=sort).agg(**kwargs))
        _iskip= False
        rdf.dataobj = self.dataobj
        return rdf
    
    def group_by(self, by=None, axis=0, level=None, as_index: bool = True, sort: bool = True, group_keys: bool = True, squeeze: bool = False, observed: bool = False):
        self.dataobj.setpattern("_agg_",chain=False)
        gpb = ix.cnvgpb_toiics(str(by))
        #self.dataobj.groupby(gpb)
        global _iexpr
        _iexpr = gpb
        estack.pushexp()
        global _iskip
        bskip = _iskip
        _iskip= True    

        s1 = super(CDIDataFrame,self).groupby(by=by, axis=axis, level=level, as_index=as_index, sort=sort, group_keys=group_keys,squeeze=squeeze,observed=observed)
        _iskip=False
        #estack.print (s1.__class__)
       
        return s1
 
    

     
    
    def fillna(self, value=None, method=None, axis=None, inplace=False, limit=None, downcast=None):
        self.dataobj.setpattern('_exp_na_') 
        if (type(value) == CDISeries):  #pd.core.series.Series):  #for single value
            for col,val in value.iteritems():
                self.dataobj.dfc.at[col,'ex'] = "iif (ISNULL("+col+"),'"+str(val)+"',"+col+")"
        elif (type(value) == dict):  #  {'A':'1','B':'2'}
            for col in value:
                val = value[col]
                self.dataobj.dfc.at[col,'ex'] = "iif (ISNULL("+col+"),'"+str(val)+"',"+col+")"
                
        else:    
            exp = ix.fillna_toiics(value)
            self.dataobj.assign(exp) 
            
        return CDIDataFrame(super(CDIDataFrame,self).fillna(value=value, method=method, axis=axis, inplace=inplace, limit=limit, downcast=downcast))

    
    def join(self, other, on=None, how='left', lsuffix='', rsuffix='', sort=False):
        self.dataobj.setpattern('_join_') 
        self.dataobj.join(other.dataobj,on,how)
        return CDIDataFrame(super(CDIDataFrame,self).join(other=other, on=on, how=how, lsuffix=lsuffix, rsuffix=rsuffix, sort=sort))
    
    def merge(self, right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_m', '_d'), copy=True, indicator=False, validate=None):
        self.dataobj.setpattern('_join_') 
        jc=on
        if (left_on != None):
            jc = left_on + '=' + right_on        
        self.dataobj.join(right.dataobj,jc,how)
        global _iskip
        _iskip= True
        df= CDIDataFrame(super(CDIDataFrame,self).merge(right, how=how, on=on, left_on=left_on, right_on=right_on, left_index=left_index, right_index=right_index, sort=sort, suffixes=suffixes, copy=copy, indicator=indicator, validate=validate))
        _iskip= False
        return df
    
    
    ##df['Sex'] = df.iif(df['Sex'] == 'male','M',df['Sex'])
    #This is a custom function extension.  calls np.where()
    def iif(self, cond,valt,valf,end=False):
        expf = ""
        expt = ""
        cond1 = ""
        if (valf.__class__ == CDISeries):
            #handle cascading iff statments for the same col
            if (end == True):
                expf =  estack.popexp()   #top of stack has last parameter
            else:
                expf =  estack.popexp()
                expf = '%col%'
        elif (valf.__class__ == np.ndarray):
            expf =  estack.popexp()
        else:
            if (valf is None):
                expf = "%col%"
            else:
                expf =  "'"+str(valf)+"'"   

        if (valt.__class__ == CDISeries):
            expt =  estack.popexp()   #top of stack has last parameter
        elif (valt.__class__ == np.ndarray):
            expf =  estack.popexp()            
        else:
            expt = "'"+str(valt)+"'"   
        if (cond.__class__ == CDISeries):
            cond1 =  estack.popexp()   #top of stack has last parameter
        else:
            cond1 = str(cond) 
        #estack.clear()    #comment out for now
        global _iexpr
        _iexpr = "iif("+cond1+","+expt+","+expf+")"
        estack.pushexp()
        #estack.print("np where "+str(cond)+ ","+str(valt))
        return np.where(cond,valt,valf)   
   
    
    
#######################################
    
class dataObject:
    def __init__(self, step, objname, cnxid):
        self.curobjname = objname
        self.curcnxid = cnxid
        self.curfilter = "TRUE"
        self.curexpr = ""
        self.curdropcols = ""
        self.curenums = ""
        self.groupcols = ""
        self.sortcols = ""
        self.rankcol = ""
        self.rankcount = ""
        
        self.aggexpr = ""
        self.curresultname = step+".csv"
        self.resultcnx = _lake_connection  
        self.stepname = step
        self.prevstep = ""
        self.joincond =""
        self.jointype=""
        self.master=""
        self.masterstep=""
        self.desc="Process '"+objname+"'."
        self.dfc = None
        self.prevdo = None  # last dataobject
        self.patternstr = ""
        #reference to functions object
        self.cexp = None 
        self.lastfilter = ""
        

    
    def __str__(self):
        return "{} ".format(self.curobjname + " in "+ self.curcnxid ) #format(self.__class__.__name__)
    
    
    def nextstep(self,step):
        do1 = dataObject(step,self.curresultname,self.resultcnx)
        do1.prevstep = self.stepname
        return do1
    
    def setdesc(self,d):
        self.desc = d
    
    def explain(self):
        print ("Task Name: "+self.stepname)
        print ("Object: "+self.curobjname)
        print ("Desc: "+ self.desc)
        print ("Destination: "+ self.curresultname)
        print ("Origin: "+ self.prevstep)
        print ("")
        print ("Filter: "+self.curfilter)
        #print ("Expr: "+self.curexpr)
        #print ("Aggregation: "+self.aggexpr)
        print ("Group by: "+self.groupcols)
        print ("Sort by: "+ self.sortcols)
        #print ("")
        if (len(self.master)>0):
            print ("Master :"+self.master)
            print ("Join Condition: "+self.joincond)
            print ("Join Type: "+self.jointype)
        print(self.dfc)
    
    def addtopattern(self,ptn):
        if (self.patternstr.find(ptn) < 0):
            self.patternstr = self.patternstr + ptn
        
    def haspattern(self,ptn):
        if (self.patternstr.find(ptn)>=0):
            return True
        else:
            return False
        
    def cancoexist(self,ptn):
        ptnstr = self.patternstr
        if (ptn == '_agg_'):
            ptnstr = ptnstr.replace('_filter_','')
            ptnstr = ptnstr.replace('_sort_','')
        elif (ptn == '_join_'):
            ptnstr = ptnstr
        elif (ptn == '_exp_'):
            ptnstr = ptnstr.replace('_filter_','')
        else:        
            ptnstr = ptnstr
        ptnstr = ptnstr.replace(' ','') 
        if len(ptnstr) > 0:
            return False
        else:
            return True
        
   
    #check if need to chain a new task
    def setpattern(self,ptn,chain=False):
        
        if (chain == False):
            self.addtopattern(ptn)
            return
        bchain= not self.cancoexist(ptn)

        if (bchain == True):
            self.transform(self.dfc)
            #copy do1 from self
            do1 =  copy.copy(self)
            print ("Chaining "+do1.stepname) 
            #clear out the expressions in dataframe
            for index, row in self.dfc.iterrows():
                row['ex'] = ""
            #reset dataobject. chain this dataset to be the result of previous step
            self.reset(self.stepname+"1",self.curresultname,self.resultcnx,self.desc+"_1")
            self.prevdo = do1
            self.prevstep = do1.stepname
        else:
            self.addtopattern(ptn)
                
        
        

    def getInput(self):
        return self.curobjname
    
    def getOutput(self):
        return self.curresultname
    
    def getfulldesc(self):
        sobj = self.curobjname
        if (len(self.prevstep) <= 0):
            sobj = sobj + '(ext)'
        d = "'Jupyter':'"+self.desc + "','Input':'"+sobj + "','Output':'"+self.curresultname+"'"
        if (len(self.joincond)>0):
            d = d+ ",'join':'"+self.master +"'"
        return d
    
    def getpattern(self):
        if (len(self.groupcols) > 0):
            return "agg"
        elif (len(self.joincond) > 0):
            return "join"
        elif (len(self.rankcol) > 0):
            return "sortrank"        
        else:
            return "standard"
    
    def reset(self,step,objname,cnxid,desc):
        self.curobjname = objname
        self.curcnxid = cnxid
        self.curfilter = "TRUE"
        self.curexpr = ""
        self.curdropcols = ""
        self.curenums = ""
        self.groupcols = ""
        self.sortcols = ""
        self.rankcol = ""
        self.rankcount = ""
        self.aggexpr = ""
        self.curresultname = step+".csv"
        self.resultcnx = _lake_connection  #"000HI50B00000000000B"  #default staging connection
        self.stepname = step
        self.prevstep = ""
        self.joincond =""
        self.jointype=""
        self.master=""
        self.masterstep=""
        self.desc=desc
        self.patternstr = ""
        self.lastfilter = ""


    def join(self,mstobj,jc,jt):
        self.joincond = jc
        if (jc.find("=") <= 0):
            self.joincond = jc+"_m = "+jc+"_d"
            self.drop(jc+"_m")
        self.master=mstobj.curobjname
        self.masterstep = mstobj.stepname
        self.jointype=jt
    
    def setresultname(self, rslt):
        self.curresultname = rslt
        
    def setresult(self, rslt,cnx):
        self.curresultname = rslt
        self.resultcnx = cnx
        
    def getresultname(self):
        return self.curresultname
        
    def drop(self,cols):
        cols = "^"+cols
        cols = cols.replace(",","|^")
        if (len(self.curdropcols) > 0):
            self.curdropcols = self.curdropcols + '|'+cols
        else:
            self.curdropcols = cols
            
    def filter(self,filt):
        if (filt != '_not_'):
            self.curfilter = self.curfilter + ' AND (' + filt + ')'
        else:
            self.curfilter = self.curfilter.replace(self.lastfilter,'NOT('+self.lastfilter+')')
        self.lastfilter = filt #save in case need to reverse last filter  
        
        
    def assign(self,expr):  
        self.curexpr = self.curexpr + ';' + expr + ';'
        
    def assignvar(self,expr):
        expr = "v("+expr+")"
        self.curexpr = self.curexpr + ';' + expr + ';'
        
    def groupby(self,flds):  
        self.groupcols = flds
        flds = flds.replace(";","=ASC;")+"=ASC" #set default sort order
        self.sortcols = flds
         
    def orderby(self,flds):
        self.sortby(flds)
        
    def sortby(self,flds):
        if (flds.find('=') <=0):
            flds = flds.replace(";","=ASC;")+"=ASC"
        self.sortcols = flds
        
    def rank(self,col,count):
        self.rankcol = col
        self.rankcount = count
            
        
    def agg(self,expr):
        self.aggexpr = self.aggexpr + ';' + expr + ';'
    
    
    def rename(self,oldname,newname):
        expr = "string (60,0) "+newname+" = "+oldname 
        self.assign(expr)
        self.drop(oldname)
        
    def replace(self,flds,expr):
        enval = 'string (10,0) Declare_%enums%={\\"port\\":\\"' 
        enval = enval + flds + '\\"};'
        #self.curenums = self.curenums+enval
        exp1 = "string(0,0) %port%_EX = "+ expr + ";"
        self.curexpr = self.curexpr + ';' + enval + ";" + exp1 + ';'
        self.drop(flds)
    #def enums from data frame. eg. from Dict
    def defenums(self,df):
        i=0
        enval=""
        columns = list(df) 
        rows = df.shape[0]
        for c in columns: 
            vals=""
            for r in range(0,rows):
                vals = vals + str(df[c][r]) + ','
            enval = enval + 'string (10,0) Declare_%enums%={\\"'
            enval = enval+ str(df.columns[i]) + '\\":\\"' + vals + '\\"};'
            i = i+1
        #print(enval)
        self.curenums = self.curenums+enval
    #def enums from series. eg. output from df.mean()
    def defenums_fromseries(self,sr):
        i=0
        enval=""
        en1=""
        en2=""
        for attr,val in sr.iteritems():
            en1 = en1 + str(attr) + ','
            en2 = en2 + str(val) + ','
        enval = enval + 'string (10,0) Declare_%enums%={\\"'
        enval = enval+ str('port') + '\\":\\"' + en1 + '\\"};'
        enval = enval + 'string (10,0) Declare_%enums%={\\"'
        enval = enval+ str('portval') + '\\":\\"' + en2 + '\\"};'
        self.curexpr = self.curexpr+enval        
        
       
    def transform(self,df1):
        allexpr = ""
        i=0
        lastdt = "integer(10,0)"  # use the last datatype if missing
        for index, row in df1.iterrows():
            thisexpr = ""
            expr1 = row['ex']
            
            dt1 = str(row['dt'])
            if (len(expr1) <= 0):
                expr1 = index
            if (len(dt1) <=3):   #cover for 'nan'
                dt1 = lastdt
            else:
                lastdt = dt1
            expr1 = expr1.replace('%col%',index)  #handle the nested iif case
            thisexpr = dt1 + index + '_EX = '+ expr1
            if (index.find("v_") == 0):
                thisexpr = dt1 + index + ' = '+ expr1
                thisexpr = 'v('+thisexpr+')'
            thisexpr = thisexpr + ";"
            allexpr = allexpr + thisexpr
            i = i+1
        if (self.getpattern() != "agg"):    
            self.curexpr = allexpr
        else:
            self.aggexpr = allexpr
        
       
    def apply(self,df1):
        self.filter(df1.filter)
        self.groupby(df1.groupbystr)
        self.sortby(df1.sortstr)
        self.transform(df1)
        
#################################################################################
_datadir = "mydata//"

class iicsintef:
    def __init__(self, uname):
        self.user = uname
        self.sessid = "none"
        self.cnid = "xxx"
        self.curtaskname = ""
        self.debug = 0
        self.lakeloc = "mydir"
        global _iskiprhs
        global _iskip
        _iskiprhs= False
        _iskip = False
        
    
    
    def __str__(self):
        return "{} ".format(self.__class__.__name__)
    
  
    #logging level  
    def log(self,l=1):
        if (l==1):
            estack.debug = 0
            estack.info = 1
        if (l==0):
            estack.debug = 0
            estack.info = 0
        if (l==2):
            estack.debug = 1
            estack.info = 1        
    
    #skip processing and assignements, filters etc.  optionally you can skip rhs evaluation alone by specifying rhs='exp'
    def SKIP(self,s=True,rhs=""):
        global _iskip
        global _iexpr
        global _iskiprhs        
        estack.clear()
        _iskiprhs = False
        if (s == False):
            _iskip = False
            return
        if (len(rhs)>0):
            _iexpr = rhs
            _iskiprhs = True
        else:
            _iexpr = "_skipit_"
        estack.pushexp()
        _iskip = True
        
    
    def listMethods(self):
        print("Logged in as "+self.user)
        print("IICS Commands:")
        print(" login()")
        print(" getAgents()")
        print(" getConnections()")
        print(" getConnectionObjects()")
        print(" getObjectFields(objname)")
        print(" showDataPreview(objectname)")
        print(" showTasks(taskname)")
        print(" run(taskname)")
        
    def list():
        print ("readFile(srcfile,filter)")
        print ("readFiles(srcdir,filter)")
        print ("readSalesForce(srcobject,soql_filter)")
        
        
    # IICS LOGIN
    def login(self):

        url = "https://dm-us.informaticacloud.com/saas/public/core/v3/login"

        payload = "{\"username\":\""+self.user + "\",\"password\":\"password\"}"
        headers = {
            'accept': "application/json",
            'content-type': "application/json"
        }

        response = requests.request("POST", url, data=payload, headers=headers)
        resp= json.loads(response.text)
        #print(json.dumps(resp, indent = 4, sort_keys=True))

        self.sessid = resp['userInfo']['sessionId']
        print(self.sessid)
        print("Login successful")
    
    def getAgents(self):
        # AGENTS

        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/runtimeEnvironment/"

        headers = {'icsessionid': self.sessid}

        response = requests.request("GET", url, headers=headers)
        resp = json.loads('{"items":' + response.text + '}')
        print ("Agents:")
        for obj in resp['items']:
            print (obj['agents'][0]['id'] + ' ' + obj['agents'][0]['name'].ljust(30) )
    
    def getConnectionObjects(self, cid):
        #cid = "000HI50B00000000000B"
        self.cnid = cid
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/connection/source/"+ cid
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        resp = json.loads(response.text)
        resp = json.loads('{"items":' + response.text + '}')
        print ("Objects:")
        for obj in resp['items']:
            print (obj['name'])   
# IICS CONNECTIONS
    def getConnections(self):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/connection"
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        cnxs = json.loads('{"items":' + response.text + '}')
        print ("Connections:")
        for conn in cnxs['items']:
            print ("{:<8}   {:<25} {:<25}".format(conn['id'], conn['name'], conn['type']))

# Object Fields
    def getObjectFields(self,objname):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/connection/source/" + self.cnid + "/field/"+objname
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        resp = json.loads(response.text)
        resp = json.loads('{"items":' + response.text + '}')
        
        if (self.debug == 1):
            print(json.dumps(resp, indent = 4, sort_keys=True))
        
        print ("Object Fields:")
        for obj in resp['items']:
            if(obj['precision'] > 0):
                print (" {:<30}   {:<25}".format(obj['name'],obj['type'])) # + '(' + obj['precision'] + ',' + obj['scale'] +')' )
            else:
                print (" {:<30}   {:<25}".format(obj['name'],obj['type']))

            
# Data Preview
    def showDataPreview(self,objname):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/connection/source/" + self.cnid + "/datapreview/"+objname
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)

        resp = json.loads(response.text)
        print ("Data Preview:")
        for fld in resp['fieldName']:
            print ('"'+fld+'",',end=" ")
        print("")
        for row in resp['rows']:
            for col in row['values']:
                print ('"'+col+'",',end=" ")
            print("")
        #print(json.dumps(resp, indent = 4, sort_keys=True))

#show Tasks
    def showTasks(self,tname):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask"
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        mcts = json.loads('{"items":' + response.text + '}')
        print ("Tasks:")
        for mct in mcts['items']:
            if (mct['name'].lower().find(tname.lower()) >= 0):
                print (mct['id'] +'\t' + mct['name'] )

                
    def setLake(self,l):
        self.lakeloc = l
                
#Show tasks from Juputer               
    def showLake(self,ftoken):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask"
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        if (len(ftoken) <= 0):
            ftoken=":"
        token1 = "Jupyter"
        mcts = json.loads('{"items":' + response.text + '}')
        print ("Lake Content:\n")
        print (" {:<26} {:<50} {:25} {:25}".format("Dataset","Description","Original Source","Step Name"))
        print("")
        for mct in mcts['items']:
            try:
                desc = mct['description']
            except:
                desc=" "
            if (len(desc)>0):
                if ((desc.lower().find(token1.lower()) >= 0) and (desc.lower().find(ftoken.lower())>0)):
                    desc = desc.replace("'",'"')
                    #print(desc)
                    try:
                        mctinfo = json.loads('{"item":{' + desc + '}}')
                        print (" {:<26} {:<50} {:25} {:25}".format(mctinfo['item']['Output'],mctinfo['item']['Jupyter'],mctinfo['item']['Input'],mct['name']))
                    except:    
                        mctinfo=""
        
      
        

#get Task ID by name               
    def getTaskID(self,name):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/name/"+name
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        resp = json.loads('{"items":' + response.text + '}')
        if (self.debug == 1):
            print(json.dumps(resp, indent = 4, sort_keys=True))
        thisid = ""
        if (response.text.find('"id":') > 0):
            thisid = resp['items']['id']
        return thisid
    
    def validateExpr(self,dataobj,expr):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/expression/validate"
        cnxid = dataobj.curcnxid  #"000HI50B00000000000B"
        obj = dataobj.curobjname
        payload = '{\"@type\":\"expressionValidation\",\"expr\":\"'+ expr + '\",\"connectionId\":\"'+cnxid+'\",\"objectName\":\"'+obj+'\"'
        payload = payload + ',\"isSourceType\":true}'
        print (payload)
        headers = {
            'icsessionid': self.sessid,
            'accept': "application/json",
            'content-type': "application/json"
            }
        response = requests.request("POST", url, data=payload, headers=headers)
        resp = response.text
        print(json.dumps(resp, indent = 4, sort_keys=True))

        
# Run task
    def execute (self,dataobj,wait=False,chain=False):
        if (chain == True):
            wait=True
            if (dataobj.prevdo != None):
                #recurssive call to run the previous step to chain the executions
                self.execute (dataobj.prevdo,wait=True,chain=True)
        rid = self.run(dataobj.stepname)  
        if (wait==True):
            if (rid > 0):
                self.monitorrun(dataobj.stepname)
                self.getactivitylog(str(rid))
          
        
    def refresh (self,dataobj):
        self.run(dataobj.prevstep)
        
        
    def run(self,taskname,wait=False):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/job"
        payload = "{\"@type\":\"job\",\"taskName\":\""+ taskname + "\",\"taskType\":\"MTT\"}"
        headers = {
            'icsessionid': self.sessid,
            'accept': "application/json",
            'content-type': "application/json"
            }
        response = requests.request("POST", url, data=payload, headers=headers)
        #print (response.text)
        if (response.text.find('taskName') > 0):
            out1 = json.loads(response.text)
            print('Task ' + out1['taskName'] + " is running...",end=" ")
            if (wait==True):
                rid =  out1['runId']
                if (rid > 0):
                    self.monitorrun(taskname)
                    self.getactivitylog(str(rid))  
            else:
                return out1['runId']
        else:
            print ("Error running task")  

    

    def monitorrun(self,tname):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/activity/activityMonitor"
        headers = {'icsessionid': self.sessid}
        running = True
        while running:
            response = requests.request("GET", url, headers=headers)
            resp = response.text
            if (resp.find(tname) <= 0):
                running=False
            print('.',end=" ")
            time.sleep(3)
        print("Completed!")

    def getactivitylog(self,tid):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/activity/activityLog?runId="+tid+"&rowLimit=1"
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)    
        resp = json.loads(response.text)
        resp= resp[0]
        #print(json.dumps(resp, indent = 4, sort_keys=True))
        errmsg = resp['entries'][0]['errorMsg']
        tname = resp['objectName']
        sttime = str(resp['startTime'])
        endtime = str(resp['endTime'])
        sttime = sttime.replace('.000Z','').replace('T',' ')  
        sto = datetime.datetime.strptime(sttime, '%Y-%m-%d %H:%M:%S')
        endtime = endtime.replace('.000Z','').replace('T',' ')  
        eto = datetime.datetime.strptime(endtime, '%Y-%m-%d %H:%M:%S')

        lapse= eto-sto

        if (errmsg.find('No errors')<0):
            print ("Task: "+tname+" End time: " + endtime + "Error: "+errmsg)
        else:
            print ("Task: "+tname+" Start time: " + sttime + ", Run time: " + str(lapse) +". Rows processed: "+str(resp['successSourceRows']))

    
###############    
            
#create task
    def readFile(self,srcname,filt):
        tid1 = "000HI50Z0000000000E6"  #get source and apply filter
        tid2 = "000HI50Z0000000000FX"
        cursourcename = srcname
        pfilter = filt
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid1

        headers = {
            'icsessionid': self.sessid,
            'accept': "application/json",
            'content-type': "application/json"
            }
        response = requests.request("GET", url, headers=headers)
        body1 = response.text
        resp = json.loads(response.text)
        #print(json.dumps(resp, indent = 4, sort_keys=True))

        body1 = body1.replace(tid1,tid2)
        body1 = body1.replace("cnxget1","jynbtask")
        body1 = body1.replace("$description$","from jupyter")
        body1 = body1.replace ("&pconnectorfilter&",pfilter);
        body1 = body1.replace("Connector_Analysis_Detail.csv",cursourcename);
        resp = json.loads(body1)
        #print(json.dumps(resp, indent = 4, sort_keys=True))

        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid2
        response = requests.request("POST", url, data=body1, headers=headers)
        resp= response.text
        if (resp.find("createTime") > 0):
            print ("Extract task 'jynbtask' created for '"+srcname + "'. Use run command to execute")
            self.curtaskname = "jynbtask"
        else:
            print ("Create failed:"+resp)
            
#create sfdc task
    def readSalesForce(self,dataobj):
        # salesforce extract
        tid1 = "000HI50Z0000000000FY"  
        tid2 = "000HI50Z0000000000FZ"
        tid2 = self.getTaskID(dataobj.stepname)
        
        cursourcename = dataobj.curobjname
        tgtname = dataobj.curresultname
        pfilter = dataobj.curfilter
        #use sfdc filter syntax
        if (pfilter == "TRUE"):
            pfilter = ""
        
        taskname = dataobj.stepname
        desc = dataobj.getfulldesc()
        
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid1

        headers = {
            'icsessionid': self.sessid,
            'accept': "application/json",
            'content-type': "application/json"
            }
        response = requests.request("GET", url, headers=headers)
        body1 = response.text
        resp = json.loads(response.text)
        #print(json.dumps(resp, indent = 4, sort_keys=True))
    
        body1 = body1.replace(tid1,tid2)
        body1 = body1.replace("sfdcextt",taskname)
        body1 = body1.replace("$description$",desc)
        body1 = body1.replace ("$sfdcflt$",pfilter);
        body1 = body1.replace("Account",cursourcename);
        resp = json.loads(body1)
        if (self.debug ==1):
            print(json.dumps(resp, indent = 4, sort_keys=True))
       

        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid2
        response = requests.request("POST", url, data=body1, headers=headers)
        resp= response.text
        
        if (self.debug ==1):
            print(json.dumps(resp, indent = 4, sort_keys=True))
            
        if (resp.find("createTime") > 0):
            print ("Extract task '" +taskname+ "' created for '"+cursourcename + "'. Use run command to execute. Results in '" +tgtname+ "'.")
            self.curtaskname = taskname
        else:
            print ("Create failed:"+resp)
     
    def gettaskid(self,dataobj):
        ptn = dataobj.getpattern()
        if (ptn == "standard"):
            return "000HI50Z0000000000G0"   #standard template
        elif (ptn == "sortrank"):
            return "000HI50Z0000000000GV"
        else:
            return "000HI50Z0000000000G2"   #agg template
        
    def getreplacetaskid(self,dataobj):
        if (dataobj.getpattern() == "standard"):
            return "000HI50Z0000000000G9"
        else:
            return "000HI50Z0000000000G3"    
        
    def gettaskname(self,dataobj):
        ptn = dataobj.getpattern()
        if (ptn == "standard"):
            return "jyptest1"
        elif (ptn == "sortrank"):
            return "jypsort1"
        else:
            return "jymagg1"
        
        
#create task for file extract    
    def readFilex(self,dataobj): 
    #PY 
        tid1 = self.gettaskid(dataobj)  #"000HI50Z0000000000G0"  
        #tid2 = self.getreplacetaskid(dataobj) #"000HI50Z0000000000G1"
        tid2 = self.getTaskID(dataobj.stepname)
        cursourcename = dataobj.curobjname
        pfilter = dataobj.curfilter
        pdropf = dataobj.curdropcols
        if (len(pdropf) <= 0):
            pdropf = "dummycol"
        pexpr=dataobj.curexpr
        if (dataobj.getpattern() == "standard"):
            pexpr = dataobj.curenums + dataobj.curexpr
  
        tgtname = dataobj.curresultname
        taskname = dataobj.stepname
        paggexpr = dataobj.aggexpr
        #for agg pattern apply the enums here
        if (dataobj.getpattern() != "standard"):
            paggexpr = dataobj.curenums + dataobj.aggexpr
        
        pgroupcols = dataobj.groupcols
        psortcols = dataobj.sortcols
        ptaskname = self.gettaskname(dataobj)
        desc = dataobj.getfulldesc()
        prankcol = dataobj.rankcol
        prankcount = str(dataobj.rankcount)
        
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid1
        headers = {
            'icsessionid': self.sessid,
            'accept': "application/json",
            'content-type': "application/json"
            }
        response = requests.request("GET", url, headers=headers)
        body1 = response.text
        resp = json.loads(response.text)
        #print(json.dumps(resp, indent = 4, sort_keys=True))

        body1 = body1.replace(tid1,tid2)
        body1 = body1.replace(ptaskname,taskname)
        body1 = body1.replace("$description$",desc)
        body1 = body1.replace ("&pconnectorfilter&",pfilter);
        body1 = body1.replace("Connector_Analysis_Detail.csv",cursourcename);
        body1 = body1.replace("&pdropfields&",pdropf);
        body1 = body1.replace("string(10,0) xyz1='a';",pexpr)
        #following needed for agg template
        body1 = body1.replace("string(10,0) xyz2='b';",paggexpr)
        body1 = body1.replace("Connector_Group=ASC",psortcols)   #Sort
        body1 = body1.replace("Connector_Group",pgroupcols)
        body1 = body1.replace("123456",prankcount)           #Rank
        body1 = body1.replace("&rankfld&",prankcol)
        
        
        if (len(tgtname) > 0):
            body1 = body1.replace("conresult.csv",tgtname)
        resp = json.loads(body1)
        if (self.debug == 1):
            print(json.dumps(resp, indent = 4, sort_keys=True))

        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid2
        response = requests.request("POST", url, data=body1, headers=headers)
        resp= response.text
        if (resp.find("createTime") > 0):
            print ("Extract task '" +taskname+ "' created for '"+cursourcename + "'. Use run command to execute. Results in '" +tgtname+ "'.")
        else:
            print ("Create failed:"+resp)
    
    # send task to IICS.  If need to get expr/agg transform set tx=True 
    def prepare(self,dataobj,tx=False):   
        estack.clear()  #clear any previous entries
        if (dataobj.prevdo != None):
            self.prepare(dataobj.prevdo,tx=False)
        if (tx == True):
            dataobj.transform(dataobj.dfc)
        if (len(dataobj.master)>0):
            self.merge(dataobj)
        elif (dataobj.stepname.find("sfdc") >= 0):  #woraround for now
            self.readSalesForce(dataobj)
        else:
            self.readFilex(dataobj)

    
    def merge(self,dataobj):
        tid1 = '000HI50Z0000000000G7'
        tid2 = '000HI50Z0000000000G8'
        tid2 = self.getTaskID(dataobj.stepname)
        filterm = "TRUE"
        pfilter = dataobj.curfilter
        pdropf = dataobj.curdropcols
        
        if (len(pdropf) <= 0):
            pdropf = "^dummycol"
        cursourcename = dataobj.curobjname
        tgtname = dataobj.curresultname
        taskname = dataobj.stepname
        ptaskname = "jypjoin1"   #self.gettaskname(dataobjd)
        joincond = dataobj.joincond
        mastername = dataobj.master
        pexpr=dataobj.curexpr
        desc = dataobj.getfulldesc()
        
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid1
        headers = {
            'icsessionid': self.sessid,
            'accept': "application/json",
            'content-type': "application/json"
            }
        response = requests.request("GET", url, headers=headers)
        body1 = response.text
        resp = json.loads(response.text)
        #print(json.dumps(resp, indent = 4, sort_keys=True))

        body1 = body1.replace(tid1,tid2)
        body1 = body1.replace(ptaskname,taskname)
        body1 = body1.replace("$description$",desc)
        body1 = body1.replace ("&pfilter1&","TRUE");
        body1 = body1.replace ("&pfilter2&",pfilter);
        body1 = body1.replace("iics_sfdc_runs.csv",cursourcename);
        body1 = body1.replace("cnxtypes.csv",mastername);
        
        body1 = body1.replace("&pdropfields&",pdropf);
        body1 = body1.replace("string(10,0) xyz1='a';",pexpr)
        #following needed for join template
        body1 = body1.replace("Target_Connection_m=Target_Connection_d",joincond)
        
        if (len(tgtname) > 0):
            body1 = body1.replace("jointgt.csv",tgtname)
        resp = json.loads(body1)
        if (self.debug == 1):
            print(json.dumps(resp, indent = 4, sort_keys=True))

        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid2
        response = requests.request("POST", url, data=body1, headers=headers)
        resp= response.text
        if (resp.find("createTime") > 0):
            print ("Extract task '" +taskname+ "' created for '"+cursourcename + "'. Use run command to execute. Results in '" +tgtname+ "'.")
        else:
            print ("Create failed:"+resp)
        
    
    
    
    def DataFrame(self,dataobj):
        pobjname = dataobj.curobjname
        pcnxid = dataobj.curcnxid
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/connection/source/" + pcnxid + "/field/"+pobjname
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        resp = json.loads(response.text)
        
        if (self.debug == 1):
            print(json.dumps(resp, indent = 4, sort_keys=True))
        
        resp = json.loads('{"items":' + response.text + '}')
        prows = []
        pdt = []
        pcols = ['dt','ex']
        vals = [ ('','' )]
        for obj in resp['items']:
            prows.append(obj['name'])
            dt = obj['pcType']
            dt = dt.replace('UNI','')
            pdt.append(dt+'('+str(obj['precision']) +',' + str(obj['scale'])+')')
            
        df1 = pd.DataFrame(columns = pcols, index=prows)
        #df1 = CDIDataFrame(pd.DataFrame(columns = pcols, index=prows))
        
        i=0
        for index, row in df1.iterrows():
            row['dt'] = pdt[i]
            row['ex'] = index
            i=i+1
        return df1    
    
    def startfrom(self, tname,step):
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/name/"+tname
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        resp = json.loads('{"items":' + response.text + '}')
        if (self.debug == 1):
            print(json.dumps(resp, indent = 4, sort_keys=True))
        bfound = False
        tgt=""
        cnx=""
        if (response.text.find('"id":') > 0):
            a = response.text.find('"targetObject":')  #look for target object
            if (a > 0):
                bfound = True
                b = response.text.find(',',a)
                tgt = response.text[a+16:b-1]
            a = response.text.find('"targetConnectionId":')  #look for target object
            if (a > 0):
                bfound = True
                b = response.text.find(',',a)
                cnx = response.text[a+22:b-1]
                
        if (bfound):
            do1 = dataObject(step,tgt,cnx)
            do1.prevstep = tname
            return do1
        else:
            print("Step not found- "+tname)
            do1 = dataObject(step,"","")
            return do1
        
    
     
    
    def metadata2(self):
        print("hello")
    
    def metadata(self):
        print("hello")
        print(dfc.columns)
    
    def getDataFrame(self,dataobj,setdt=False):
        pobjname = dataobj.curobjname
        pcnxid = dataobj.curcnxid
        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/connection/source/" + pcnxid + "/field/"+pobjname
        headers = {'icsessionid': self.sessid}
        response = requests.request("GET", url, headers=headers)
        resp = json.loads(response.text)
        
        if (self.debug == 1):
            print(json.dumps(resp, indent = 4, sort_keys=True))
        
        resp = json.loads('{"items":' + response.text + '}')
        prows = []
        pdt = []
        pcols = ['dt','ex']
        vals = [ ('','' )]
        for obj in resp['items']:
            prows.append(obj['name'])
            dt = obj['pcType']
            dt = dt.replace('UNI','')
            pdt.append(dt+'('+str(obj['precision']) +',' + str(obj['scale'])+')')
            
        cdidf = CDIDataFrame(pd.read_csv(_datadir+pobjname))
        #remove spaces etc
        cdidf.columns =[column.replace(" ", "_") for column in cdidf.columns]
        cdidf.columns =[column.replace("/", "_") for column in cdidf.columns]
        cdidf.columns =[column.replace("#", "_") for column in cdidf.columns]
        
        dataobj.dfc = pd.DataFrame(columns = pcols, index=prows)
        #df1 = CDIDataFrame(pd.DataFrame(columns = pcols, index=prows))
        
        i=0
        for index, row in dataobj.dfc.iterrows():
            row['dt'] = pdt[i]
            row['ex'] = index
            i=i+1
        cdidf.dataobj = dataobj
        if (setdt == True):
            self.setcoldatatypes(cdidf)
        
        estack.clear()  #clear any previous entries
        return cdidf   
    
    #set datatypes of IICS columns based on pandas data types
    def setcoldatatypes(self,cdf):
        curcols = str(cdf.dataobj.dfc.index.values)         
        int_cols = cdf.select_dtypes(include=['int64']).columns        
        for col in int_cols: 
            if (curcols.find("'"+col+"'") >0):
                cdf.dataobj.dfc.at[col,'dt'] = 'integer(10,0)'
        float_cols = cdf.select_dtypes(include=['float64']).columns
        for col in float_cols:
            if (curcols.find("'"+col+"'") >0):
                cdf.dataobj.dfc.at[col,'dt'] = 'decimal(10,2)'            
        

#make copy of existing IICS task and change source etc..
    def copytask(self,tname,tcopyname,src=None,newsrc=None): 

        tid1 = self.getTaskID(tname)
        tid2 =  self.getTaskID(tcopyname)
        if (len(tid1) <= 0):
            print("Task not found!")
            return

        cursourcename = src

        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid1
        headers = {
            'icsessionid': self.sessid,
            'accept': "application/json",
            'content-type': "application/json"
            }
        response = requests.request("GET", url, headers=headers)
        body1 = response.text
        resp = json.loads(response.text)
        #print(json.dumps(resp, indent = 4, sort_keys=True))

        body1 = body1.replace(tid1,tid2)
        body1 = body1.replace('"'+tname+'"','"'+tcopyname+'"')
        body1 = body1.replace(src,newsrc);

        resp = json.loads(body1)
        if (self.debug == 1):
            print(json.dumps(resp, indent = 4, sort_keys=True))

        url = "https://usw3.dm-us.informaticacloud.com/saas/api/v2/mttask/"+tid2
        response = requests.request("POST", url, data=body1, headers=headers)
        resp= response.text
        if (resp.find("createTime") > 0):
            print ("Extract task '" +tcopyname+ "' created for '"+cursourcename + "'. Use run command to execute. Results in '" +tname+ ".csv'.")
        else:
            print ("Create failed:"+resp)




        
        
        
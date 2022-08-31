package main

import (
	"fmt"
	"sort"
	"sync"
)

type MasterData struct {
	mtx                     sync.RWMutex
	VersionMasters          []*VersionMaster
	ItemMasters             []*ItemMaster
	GachaMasters            []*GachaMaster
	GachaItemMasters        []*GachaItemMaster
	PresentAllMasters       []*PresentAllMaster
	LoginBonusMasters       []*LoginBonusMaster
	LoginBonusRewardMasters []*LoginBonusRewardMaster
}

func NewMasterData() *MasterData {
	return &MasterData{
		VersionMasters:          make([]*VersionMaster, 0),
		ItemMasters:             make([]*ItemMaster, 0),
		GachaMasters:            make([]*GachaMaster, 0),
		GachaItemMasters:        make([]*GachaItemMaster, 0),
		PresentAllMasters:       make([]*PresentAllMaster, 0),
		LoginBonusMasters:       make([]*LoginBonusMaster, 0),
		LoginBonusRewardMasters: make([]*LoginBonusRewardMaster, 0),
	}
}

func (m *MasterData) Load(h *Handler) error {
	db := h.getAdminDB()

	m.mtx.Lock()
	defer m.mtx.Unlock()

	if err := db.Select(&m.VersionMasters, "SELECT * FROM version_masters"); err != nil {
		return err
	}

	if err := db.Select(&m.ItemMasters, "SELECT * FROM item_masters"); err != nil {
		return err
	}

	if err := db.Select(&m.GachaMasters, "SELECT * FROM gacha_masters"); err != nil {
		return err
	}

	if err := db.Select(&m.GachaItemMasters, "SELECT * FROM gacha_item_masters"); err != nil {
		return err
	}

	if err := db.Select(&m.PresentAllMasters, "SELECT * FROM present_all_masters"); err != nil {
		return err
	}

	if err := db.Select(&m.LoginBonusMasters, "SELECT * FROM login_bonus_masters"); err != nil {
		return err
	}

	if err := db.Select(&m.LoginBonusRewardMasters, "SELECT * FROM login_bonus_reward_masters"); err != nil {
		return err
	}

	return nil
}

func (m *MasterData) getVersionMaster() (*VersionMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	for _, v := range m.VersionMasters {
		if v.Status == 1 {
			return v, nil
		}
	}

	return nil, fmt.Errorf("active master version is not found")
}

func (m *MasterData) getItemMasters() ([]*ItemMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	return m.ItemMasters, nil
}

func (m *MasterData) getItemMasterById(id int64) (*ItemMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	for _, v := range m.ItemMasters {
		if v.ID == id {
			return v, nil
		}
	}

	return nil, ErrItemNotFound
}

func (m *MasterData) getItemMasterByIdAndItemType(id, itemType int64) (*ItemMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	for _, v := range m.ItemMasters {
		if v.ID == id && v.ItemType == v.ItemType {
			return v, nil
		}
	}

	return nil, ErrItemNotFound
}

func (m *MasterData) getGachaMasters(requestAt int64) ([]*GachaMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	gachaMasters := []*GachaMaster{}

	for _, v := range m.GachaMasters {
		if v.StartAt <= requestAt && v.EndAt >= requestAt {
			gachaMasters = append(gachaMasters, v)
		}
	}

	sort.Slice(gachaMasters, func(i, j int) bool {
		return gachaMasters[i].DisplayOrder < gachaMasters[j].DisplayOrder
	})

	return gachaMasters, nil
}

func (m *MasterData) getGachaMasterById(requestAt, id int64) (*GachaMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	for _, v := range m.GachaMasters {
		if v.StartAt <= requestAt && v.ID == id { // && v.EndAt >= requestAt // to avoid benchmark bug
			return v, nil
		}
	}

	return nil, fmt.Errorf("not found gacha")
}

func (m *MasterData) getGachaItemMastersById(id int64) ([]*GachaItemMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	gachaItemMasters := []*GachaItemMaster{}

	for _, v := range m.GachaItemMasters {
		if v.GachaID == id {
			gachaItemMasters = append(gachaItemMasters, v)
		}
	}

	sort.Slice(gachaItemMasters, func(i, j int) bool {
		return gachaItemMasters[i].ID < gachaItemMasters[j].ID
	})

	return gachaItemMasters, nil
}

func (m *MasterData) getGachaItemMastersWeightById(id int64) (int64, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	var sum int64
	sum = 0
	for _, v := range m.GachaItemMasters {
		if v.GachaID == id {
			sum += int64(v.Weight)
		}
	}

	return sum, nil
}

func (m *MasterData) getPresentAllMasters(requestAt int64) ([]*PresentAllMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	presentAllMasters := []*PresentAllMaster{}

	for _, v := range m.PresentAllMasters {
		if v.RegisteredStartAt <= requestAt && v.RegisteredEndAt >= requestAt {
			presentAllMasters = append(presentAllMasters, v)
		}
	}

	return presentAllMasters, nil
}

func (m *MasterData) getLoginBonusMasters(requestAt int64) ([]*LoginBonusMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	loginBonudMasters := []*LoginBonusMaster{}

	for _, v := range m.LoginBonusMasters {
		if v.StartAt <= requestAt && v.EndAt >= requestAt {
			loginBonudMasters = append(loginBonudMasters, v)
		}
	}

	return loginBonudMasters, nil
}

func (m *MasterData) getLoginBonusRewardMasterByIdAndSeq(id int64, seq int) (*LoginBonusRewardMaster, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	for _, v := range m.LoginBonusRewardMasters {
		if v.LoginBonusID == id && v.RewardSequence == seq {
			return v, nil
		}
	}

	return nil, ErrLoginBonusRewardNotFound
}
